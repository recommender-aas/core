package gr.ml.analytics.service.contentbased

import com.typesafe.config.Config
import gr.ml.analytics.service.{SinkNew, SourceNew}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class CBFJobNew(val config: Config,
             val source: SourceNew,
             val sink: SinkNew,
             val params: Map[String, Any],
             val pipeline: Pipeline)(implicit val sparkSession: SparkSession) {

  private val lastNSeconds = params.get("hb_last_n_seconds").get.toString.toInt
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
//  private val cbPredictionsTable: String = config.getString("cassandra.cb_predictions_table")
  private val cbPredictionsColumn: String = config.getString("cassandra.cb_predictions_column")

  import sparkSession.implicits._

  def run(): Unit = {
    val itemAndFeaturesDF = source.getAllItemsAndFeatures()

    for (userId <- source.getUserIdsForLastNSeconds(lastNSeconds)) {
      // each user requires a separate model
      // CBF steps:
      // 1. select DataFrame of (label, features) for a given user
      // 2. train model using dataset from step 1
      // 3. get not rated items
      // 4. perform predictions using created model

      // TODO Slow. Improve performance
      val trainingDF = source.getAllRatings(ratingsTable) // TODO Q4
        .filter($"userid" === userId)
        .select("itemid", "rating")
        .as("d1").join(itemAndFeaturesDF.as("d2"), $"d1.itemid" === $"d2.itemid")
        .select($"d1.rating".as("label"),
          $"d2.features".as("features"))

      val model = pipeline.fit(trainingDF)

      val notRatedDF = source.getUserItemPairsToRate(userId) // TODO Q5
        .as("d1").join(itemAndFeaturesDF.as("d2"), $"d1.itemId" === $"d2.itemid")
        .select($"d1.itemId".as("itemId"), $"d1.userId".as("userId"),
          $"d2.features".as("features"))

      val predictedRatingsDS = model.transform(notRatedDF)
        .filter(col("prediction").isNotNull)
        .select("userId", "itemId", "prediction")

      predictedRatingsDS.collect().foreach(r => {
        val userId = r.getInt(0)
        val itemId = r.getInt(1)
        val prediction = r.getDouble(2).toFloat
        sink.storePrediction(userId, itemId, prediction, cbPredictionsColumn)
      })
    }
  }
}


object CBFJobNew {

  def apply(config: Config,
            source: SourceNew,
            sink: SinkNew,
            pipeline: Pipeline,
            params: Map[String, Any]
           )(implicit sparkSession: SparkSession): CBFJobNew = {

    new CBFJobNew(config, source, sink, params, pipeline)
  }


}