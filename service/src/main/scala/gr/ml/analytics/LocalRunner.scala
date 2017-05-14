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

    val lastNSeconds = params.get("hb_last_n_seconds").get.toString.toInt
    val userIds: Set[Int] = source.getUserIdsForLastNSeconds(lastNSeconds)
    val cfJob = CFJobNew(config, source, sink, params, userIds)
    val cbfJob = CBFJobNew(config, source, sink, pipeline, params, userIds)
    val popularItemsJob = PopularItemsJobNew(source, config)
    //    val clusteringJob = ItemClusteringJob(source, sink, config)

    popularItemsJob.run() // TODO should we save recent items too ?

    do {
      cfJob.run()
      cbfJob.run()
//      clusteringJob.run()


      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)
  }


  def main(args: Array[String]): Unit = {

    timed(() => run())

  }
}
