package gr.ml.analytics.service

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.service.cf.{CFJobNew, CFPredictionService}
import gr.ml.analytics.service.contentbased.{CBFJobNew, CBPredictionService, RandomForestEstimatorBuilder}
import gr.ml.analytics.service.popular.PopularItemsJobNew
import gr.ml.analytics.util._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class HybridServiceNew(val subRootDir: String,
                    val config: Config,
                    val source: SourceNew,
                    val sink: SinkNew,
                    val paramsStorage: ParamsStorage,
                    val userIds: Set[Int])(implicit val sparkSession: SparkSession) extends Constants{

  import sparkSession.implicits._

  val cfPredictionService = new CFPredictionService(subRootDir)
  val cbPredictionService = new CBPredictionService(subRootDir)
  val csv2svmConverter = new CSVtoSVMConverter(subRootDir)

  private val cfPredictionsColumn: String = config.getString("cassandra.cf_predictions_column")
  private val cbPredictionsColumn: String = config.getString("cassandra.cb_predictions_column")
  private val hybridPredictionsColumn: String = config.getString("cassandra.hybrid_predictions_column")

  def run(cbPipeline: Pipeline): Unit ={
    val popularItemsJob = PopularItemsJobNew(source, config)
    popularItemsJob.run()

    while(true) {
      Thread.sleep(5000) // can be increased for production

      CFJobNew(config, source, sink, paramsStorage.getParams(), userIds).run() // TODO add logging somehow
      CBFJobNew(config, source, sink, cbPipeline, paramsStorage.getParams(), userIds).run()

      val collaborativeWeight = paramsStorage.getParams()("hb_collaborative_weight").toString.toLong
      combinePredictions(collaborativeWeight)
    }

  }

  def prepareNecessaryFiles(): Unit ={
    val startTime = System.currentTimeMillis()
    if(! new File(String.format(moviesWithFeaturesPath, subRootDir)).exists())
      new GenresFeatureEngineering(subRootDir).createAllMoviesWithFeaturesFile()
    val lastNSeconds = paramsStorage.getParams()("hb_last_n_seconds").toString.toInt
    val userIds = source.getUserIdsForLastNSeconds(lastNSeconds)
    userIds.foreach(csv2svmConverter.createSVMRatingsFileForUser)
    if(! new File(allMoviesSVMPath).exists())
      csv2svmConverter.createSVMFileForAllItems()
    val finishTime = System.currentTimeMillis()

    LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: Startup time took: " + (finishTime - startTime) + " millis.")
  }

  def multiplyByWeight(predictions: List[List[String]], weight: Double): List[List[String]] ={
    predictions.map(l=>List(l(0), l(1), (l(2).toDouble * weight).toString))
  }

  def combinePredictions(collaborativeWeight: Double): Unit ={
  for(userId <- userIds){
      combinePredictionsForUser(userId, collaborativeWeight)
    }
  }

  def combinePredictionsForUser(userId: Int, collaborativeWeight: Double): Unit ={
    val cfPredictionsDF = source.getPredictionsForUser(userId, cfPredictionsColumn)
      .withColumn("prediction", $"prediction" * collaborativeWeight)
    val cbPredictionsDF = source.getPredictionsForUser(userId, cbPredictionsColumn)
      .withColumn("prediction", $"prediction" * (1-collaborativeWeight))

    val hybridPredictions = cfPredictionsDF
      .select("key","userid", "itemid", "prediction")
      .as("d1").join(cbPredictionsDF.as("d2"), $"d1.key" === $"d2.key")
      .withColumn("hybrid_prediction", $"d1.prediction" + $"d2.prediction")
      .select($"d1.key", $"d2.userid", $"d2.itemid", $"hybrid_prediction".as("prediction"))
      .sort($"prediction".desc)
      .limit(1000) // TODO extract to config

    // TODO we can perform summing here:
    hybridPredictions.collect().foreach(r => {
      val userId = r.getInt(0)
      val itemId = r.getInt(1)
      val prediction = r.getFloat(2)
      sink.updatePredictions(userId, itemId, prediction, hybridPredictionsColumn)
    })

    val finalPredictedIDs = hybridPredictions.select("itemid").collect().map(r => r.getInt(0)).toList
//    sink.storeRecommendedItemIDs(userId, finalPredictedIDs) // TODO save as list, not : separated String!!
  }
}

object HybridServiceRunnerNew extends App with Constants{
  Util.windowsWorkAround()
  Util.loadAndUnzip(mainSubDir)
  //      val cbPipeline = LinearRegressionWithElasticNetBuilder.build(userId)
  val cbPipeline = RandomForestEstimatorBuilder.build(mainSubDir)
  //      val cbPipeline = GeneralizedLinearRegressionBuilder.build(userId)

  implicit val sparkSession = SparkUtil.sparkSession()

  val config = ConfigFactory.load("application.conf")

  val featureExtractor = new RowFeatureExtractor
  val source = new CassandraSource(config, featureExtractor)
  val sink = new CassandraSink(config)
  val paramsStorage: ParamsStorage = new RedisParamsStorage
  val hb = new HybridService(mainSubDir, config, source, sink, paramsStorage)
  hb.run(cbPipeline)
}
