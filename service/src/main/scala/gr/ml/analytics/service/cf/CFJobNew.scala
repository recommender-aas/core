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
            val params: Map[String, Any],
            val userIds: Set[Int])(implicit val sparkSession: SparkSession) {

  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val cfPredictionsColumn: String = config.getString("cassandra.cf_predictions_column")


  /**
    * Spark job entry point
    */
  def run(): Unit = {
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

    for(userId <- userIds){
     val notRatedPairsDF = source.getNotRatedItemsWithFeatures(userId).select("userId", "itemId")
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull && !col("prediction").isNaN)
        .select("userid", "itemid", "prediction")

      predictedRatingsDS.collect().foreach(r => {
        val userId = r.getInt(0)
        val itemId = r.getInt(1)
        val prediction = r.getFloat(2)
        sink.updatePredictions(userId, itemId, prediction, cfPredictionsColumn)
      })

      sink.storeRecommendedItemIDs(userId)
    }
  }
}

object CFJobNew extends Constants {

  def apply(config: Config,
            source: SourceNew,
            sink: SinkNew,
            params: Map[String, Any],
            userIds: Set[Int])(implicit sparkSession: SparkSession): CFJobNew = {

    new CFJobNew(config, source, sink, params, userIds)
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
