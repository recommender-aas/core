package gr.ml.analytics

import java.time.{Duration, Instant}

import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service._
import gr.ml.analytics.service.HybridServiceRunner.mainSubDir
import gr.ml.analytics.service.cf.CFJobNew
import gr.ml.analytics.util.RedisParamsStorage
import gr.ml.analytics.service.contentbased.{CBFJobNew, LinearRegressionWithElasticNetBuilder}
import gr.ml.analytics.service.popular.PopularItemsJobNew
import gr.ml.analytics.util.{ParamsStorage, Util}
import org.apache.spark.sql.SparkSession

/**
  * Run Spark jobs in local mode periodically.
  */
object LocalRunner {

  val ITERATIVE = false
  val INTERVAL_MS = 5000

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark Recommendation Service")
      .getOrCreate
  }

  def timed(metered: () => Unit): Unit = {
    val start = Instant.now()

    metered.apply()

    val end = Instant.now()

    println(s"Start time $start")
    println(s"End time $end")

    val duration = Duration.between(start, end)
    val s = duration.getSeconds

    println(s"Elapsed time ${String.format("%s:%s:%s", (s / 3600).toString, ((s % 3600) / 60).toString, (s % 60).toString)}")
  }


  def run(): Unit = {
    Util.windowsWorkAround()

    val config = ConfigFactory.load("application.conf")

    val schemaId = 0

    val paramsStorage: ParamsStorage = new RedisParamsStorage

    implicit val spark = getSparkSession
    import spark.implicits._

    val params = paramsStorage.getParams()

    val featureExtractor = new RowFeatureExtractor

    // function to get feature ration from Redis
    //    def getRatio(featureName: String): Double = {
    //      val itemFeatureRationPattern = "schema.%s.item.feature.%s.ratio"
    //      paramsStorage.getParam(itemFeatureRationPattern.format(schemaId, featureName))
    //        .toString.toDouble
    //    }
    //
    //    val featureExtractor = new WeightedFeatureExtractor(getRatio)

    val pipeline = LinearRegressionWithElasticNetBuilder.build("")

    val source = new CassandraSourceNew(config, featureExtractor)
    val sink = new CassandraSinkNew(config)

    val cfJob = CFJobNew(config, source, sink, params)
    val cbfJob = CBFJobNew(config, source, sink, pipeline, params)
    val popularItemsJob = PopularItemsJobNew(source, config)
//    val clusteringJob = ItemClusteringJob(source, sink, config)

    val hb = new HybridServiceNew(mainSubDir, config, source, sink, paramsStorage)

    // TODO when adding new rating, we are checking if this is a new user (from users table)
    // if so, we insert into items_not_rated_by_user and items_not_rated_by_user_cb table
    // all the items for this user
    // when we get a new item, we insert into items_not_rated_by_user and items_not_rated_by_user_cb table
    // combinations of this item and all the users
    // when new rating is coming we are removing this item + user from those 2 tables...

/*
    val asDense = udf((array: scala.collection.mutable.WrappedArray[Double]) => Vectors.dense(array.toArray))
    val result = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "test_vector", "keyspace" -> "rs_keyspace"))
      .load()
      .withColumn("features_vector", asDense(col("features")))
      .select(col("itemid"), col("features_vector").as("features"))
*/

    do {

      // TODO try if applying of asDense method to tuple is faster that applying it allready to column (CBFJob)
      // But be careful so that source.getAllRatings in CFJob does not work mush slower!!!



      cfJob.run()
      cbfJob.run()
      popularItemsJob.run()
      hb.combinePredictionsForLastUsers(0.1)
//      clusteringJob.run()


      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)
  }


  def main(args: Array[String]): Unit = {

    timed(() => run())

  }
}
