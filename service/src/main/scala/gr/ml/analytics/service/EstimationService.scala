package gr.ml.analytics.service

import java.util.UUID

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service.cf.CFJob
import gr.ml.analytics.service.contentbased.{CBFJob, DecisionTreeRegressionBuilder, LinearRegressionWithElasticNetBuilder, RandomForestEstimatorBuilder}
import gr.ml.analytics.service.popular.PopularItemsJob
import gr.ml.analytics.util._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, concat, lit}

object EstimationService extends App with Constants{
  val trainFraction = 0.7
  val upperFraction = 0.4
  val lowerFraction = 0.4
  val subRootDir = "precision"

  implicit val sparkSession = SparkUtil.sparkSession()

  import sparkSession.implicits._
  import org.apache.spark.sql.cassandra._

  val config = ConfigFactory.load("application.conf")
  val keySpace = config.getString("cassandra.keyspace")
  val ratingsTable = config.getString("cassandra.ratings_table")
  val trainRatingsTable = config.getString("cassandra.train_ratings_table")
  val testRatingsTable = config.getString("cassandra.test_ratings_table")
  val hybridPredictionsTable = config.getString("cassandra.hybrid_predictions_table")


  val featureExtractor = new RowFeatureExtractor

  val source = new CassandraSource(config, featureExtractor)
  val sink = new CassandraSink(config)

  val paramsStorage: ParamsStorage = new RedisParamsStorage
  val lastNSeconds = paramsStorage.getParams()("hb_last_n_seconds").toString.toInt
  val hb = new HybridService(mainSubDir, config, source, sink, paramsStorage)

  Util.loadAndUnzip(subRootDir)
  PopularItemsJob(source, config).run()

  divideRatingsIntoTrainAndTest()
  var bestAccuracy = 0.0
  var bestParams = (0.0, 0.0)
  var bestCBPipeline: Pipeline = null

  val pipelines:List[Pipeline] = List(
    LinearRegressionWithElasticNetBuilder.build(subRootDir)//,
//    RandomForestEstimatorBuilder.build(subRootDir),
//    DecisionTreeRegressionBuilder.build(subRootDir)
    )

  pipelines.foreach(pipeline => {
//    CFJob(config, source, sink, paramsStorage.getParams()).run()
//    CBFJob(config, source, sink, pipeline, paramsStorage.getParams()).run()

    (0.1 to 1.0 by 0.05).foreach(cfWeight => {
      hb.combinePredictionsForLastUsers(cfWeight)
      val accuracy = estimateAccuracy(upperFraction, lowerFraction)
      println("LinearRegressionWithElasticNetBuilder:: Weights: " + cfWeight + ", " + (1-cfWeight) + " => Accuracy: " + accuracy)
      if(accuracy > bestAccuracy){
        bestAccuracy = accuracy
        bestParams = (cfWeight, 1-cfWeight)
        bestCBPipeline = pipeline
      }
    })
  })

  println("Best Accuracy is " + bestAccuracy + " for pipeline: " + bestCBPipeline.getStages +  " and params " + bestParams)

  def divideRatingsIntoTrainAndTest(): Unit ={
    val allRatings = source.getRatings(ratingsTable).collect().map(r => List(r(0), r(1), r(2), r(3))).toList
    sink.clearTable(ratingsTable) // we are going to only save train ratings into the ratings table
    sink.clearTable(testRatingsTable)

    allRatings.groupBy(l=>l(0)).foreach(l=>{
      val trainTestTuple = l._2.splitAt((l._2.size * trainFraction).toInt)

      // TODO can I do something generic?
      trainTestTuple._1
        .map(l => (l(0).toString().toInt, l(1).toString().toInt, l(2).toString().toDouble, l(3).toString().toLong))
        .toDF("userid", "itemid", "rating", "timestamp")
        .select("userid", "itemid", "rating", "timestamp")
        .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
        .write.mode("append")
        .cassandraFormat(ratingsTable, keySpace)
        .save()

      trainTestTuple._2
        .map(l => (l(0).toString().toInt, l(1).toString().toInt, l(2).toString().toDouble, l(3).toString().toLong))
        .toDF("userid", "itemid", "rating", "timestamp")
        .select("userid", "itemid", "rating", "timestamp")
        .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
        .write.mode("append")
        .cassandraFormat(testRatingsTable, keySpace)
        .save()
    })
  }

  def getNumberOfTrainRatings(): Int ={
    val ratingsReader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val trainRatingsNumber = ratingsReader.all().size
    ratingsReader.close()
    trainRatingsNumber
  }

  def estimateAccuracy(upperFraction: Double, lowerFraction: Double): Double ={

    val testRatings = source.getRatings(testRatingsTable)
      .select("userid", "itemid", "rating")
      .collect()
      .map(r => List(r(0).toString, r(1).toString, r(2).toString)).toList

    var allFinalPredictions: List[List[String]] = List()
    source.getUserIdsForLastNSeconds(lastNSeconds)
      .foreach(userId =>     {   // TODO actually I just need the whole table !!!
        allFinalPredictions ++=
          source.getPredictionsForUser(userId, hybridPredictionsTable)
            .select("userid", "itemid", "prediction")
            .collect()
            .map(r => List(r(0).toString, r(1).toString, r(2).toString)).toList
      })

    val testRatingsLabeled = labelAsPositiveOrNegative(testRatings, upperFraction, lowerFraction)
    val finalPredictionsLabeled = labelAsPositiveOrNegative(allFinalPredictions, upperFraction, lowerFraction)
    val bothTestRatingsAndPredictionsLabeled = testRatingsLabeled ++ finalPredictionsLabeled

    val testPredictionPairs = bothTestRatingsAndPredictionsLabeled.groupBy(l=>(l(0),l(1))).filter(t=>t._2.size == 2)

    val correctlyPredicted = testPredictionPairs.filter(t=>t._2(0)(3)==t._2(1)(3)).size

    correctlyPredicted.toDouble/testPredictionPairs.size
  }

  def labelAsPositiveOrNegative(ratings: List[List[String]], upperFraction: Double, lowerFraction: Double): List[List[_]] ={
    val userIdInd = 0
    val itemIdInd = 1
    val ratingInd = 2

    val justRatings = ratings.filter(l => l(userIdInd) != "userId").map(l => l(ratingInd).toDouble)
    val ratingRange = justRatings.max - justRatings.min
    val upperLimit = justRatings.min + (1.0-upperFraction)*ratingRange
    val lowerLimit = justRatings.min + lowerFraction*ratingRange

    val labeledRatings = ratings.filter(l=>l(userIdInd)!="userId").filter(l=>l(ratingInd).toDouble >= upperLimit || l(ratingInd).toDouble < lowerLimit)
        .map(l=>List(l(userIdInd), l(itemIdInd), l(ratingInd), if(l(ratingInd).toDouble >= upperLimit) 1 else 0))

    labeledRatings
  }
}
