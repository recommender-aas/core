package gr.ml.analytics

import java.time.{Duration, Instant, LocalDateTime, Period}

import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service._
import gr.ml.analytics.service.HybridServiceRunner.mainSubDir
import gr.ml.analytics.service.cf.CFJob
import gr.ml.analytics.service.clustering.ItemClusteringJob
import gr.ml.analytics.util.RedisParamsStorage
import gr.ml.analytics.service.contentbased.{CBFJob, LinearRegressionWithElasticNetBuilder}
import gr.ml.analytics.service.popular.PopularItemsJob
import gr.ml.analytics.util.{ParamsStorage, Util}
import org.apache.spark.sql.SparkSession

/**
  * Run Spark jobs in local mode periodically.
  */
object LocalRunner {

  val ITERATIVE = true
  val INTERVAL_MS = 24 * 3600 * 1000  // every day

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

    val source = new CassandraSource(config, featureExtractor)
    val sink = new CassandraSink(config)

    val cfJob = CFJob(config, source, sink, params)
    val cbfJob = CBFJob(config, source, sink, pipeline, params)
//    val popularItemsJob = PopularItemsJob(source, config)
//    val clusteringJob = ItemClusteringJob(source, sink, config)

    val hb = new HybridService(mainSubDir, config, source, sink, paramsStorage)

    var counter = 0

    do {

      cfJob.run()
      cbfJob.run()
//      popularItemsJob.run()
      hb.combinePredictionsForLastUsers(0.1)
//      clusteringJob.run()

      counter += 1
      println(s"finished $counter iteration. Sleeping. . .")
      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)
  }


  def main(args: Array[String]): Unit = {

    timed(() => run())

  }
}
