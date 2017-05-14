package gr.ml.analytics.service

import org.apache.spark.sql.DataFrame

trait SinkNew {
  /**
    * General method for storing predictions (CF, CB and final)
    */
  def updatePredictions(userId: Int, itemId: Int, predictedValue: Float, predictionColumn: String)

  def storeRecommendedItemIDs(userId: Int)

  def clearTable(table: String)

  def storeItemClusters(itemClustersDF: DataFrame)

}
