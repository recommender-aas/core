package gr.ml.analytics.service

import org.apache.spark.sql.DataFrame

trait SourceNew {
  /**
    * @return DataFrame of (userId: Int, itemId: Int, rating: float) triples to train model
    */
  def getAllRatings(tableName: String): DataFrame

  /**
    * @return Set of userIds the performed latest ratings
    */
  def getUserIdsForLastNSeconds(seconds : Int): Set[Int]

  /**
    * @return DataFrame of itemIds and userIds for rating (required by CF job)
    */
  def getNotRatedItems(userId: Int): DataFrame

  def getNotRatedItemsWithFeaturesMap(userId: Int): Map[Int, List[Double]]

  def getNotRatedItemsWithFeatures(userId: Int): DataFrame
  /**
    * @return DataFrame of itemIds and numeric features
    */
  def getAllItemsAndFeatures(): DataFrame

  def getPredictionsForUser(userId: Int, table: String): DataFrame
}