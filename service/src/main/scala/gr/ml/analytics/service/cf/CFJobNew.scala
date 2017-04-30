package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.service._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJobNew(val config: Config,
            val source: SourceNew,
            val sink: SinkNew,
            val params: Map[String, Any])(implicit val sparkSession: SparkSession) {

  private val lastNSeconds = params.get("hb_last_n_seconds").get.toString.toInt
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")


  /**
    * Spark job entry point
    */
  def run(): Unit = {
    sink.clearTable(cfPredictionsTable)
    val allRatingsDF = source.getAllRatings(ratingsTable)
      .select("userId", "itemId", "rating")

    val rank = params("cf_rank").toString.toInt
    val regParam = params("cf_reg_param").toString.toDouble
    val als = new ALS()
      .setMaxIter(2)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    for(userId <- source.getUserIdsForLastNSeconds(lastNSeconds)){
     val notRatedPairsDF = source.getUserItemPairsToRate(userId) // TODO Q3 + We need some query to get all available itemIds (to insert when we have a new user...)
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull)
        .select("userid", "itemid", "prediction")
        .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))

      sink.storePredictions(predictedRatingsDS, cfPredictionsTable)
    }
  }
}

object CFJobNew extends Constants {

  def apply(config: Config,
            source: SourceNew,
            sink: SinkNew,
            params: Map[String, Any])(implicit sparkSession: SparkSession): CFJobNew = {

    new CFJobNew(config, source, sink, params)
  }

  private def readModel(spark: SparkSession): ALSModel = {
    val model = ALSModel.load(String.format(collaborativeModelPath, mainSubDir))
    model
  }

  private def writeModel(spark: SparkSession, model: ALSModel): ALSModel = {
    model.write.overwrite().save(String.format(collaborativeModelPath, mainSubDir))
    model
  }
}
