package gr.ml.analytics.batch.cf

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._


trait StaticConfig {
  val userIdCol = "userid"
  val itemIdCol = "itemid"
  val ratingCol = "rating"
  val timestampCol = "timestamp"
  val predictionCol = "prediction"
  val recommendedItemIdsCol = "recommended_item_ids"
  val ratingsKeyCol = "key"
}


trait Source {
  /**
    * @return DataFrame of (userId: Int, itemId: Int, rating: float, timestamp: long) to train model
    */
  def getTrainingRatings: DataFrame

  /**
    * @return Set of userIds the performed latest ratings
    */
  def getUserIds: Set[Int]
}


trait Sink {
  def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int], latestTimestamp: Long)
}


class CassandraSource(val keyspace: String,
                      val ratingsTable: String,
                      val startPredictionTimestamp: Option[Long])(implicit val sparkSession: SparkSession) extends Source with StaticConfig {

  private lazy val ratingsDF = sparkSession.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
    .load()
    .select(userIdCol, itemIdCol, ratingCol, timestampCol)
    .cache()

  override def getTrainingRatings: DataFrame = ratingsDF

  override def getUserIds: Set[Int] = {
    val userIdsDF = startPredictionTimestamp match {
      case Some(timestamp) =>
        ratingsDF.filter(col(timestampCol) > timestamp)
          .select(userIdCol).distinct()
      case None =>
        ratingsDF.select(userIdCol).distinct()
    }

    userIdsDF.collect().map(r => r.getInt(0)).toSet
  }
}


class CassandraSink(val keyspace: String,
                    val recommendationsTable: String)(implicit val sparkSession: SparkSession) extends Sink with StaticConfig {

  override def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int], latestTimestamp: Long): Unit = {
    val recommendedIDsString = recommendedItemIds.toArray.mkString(":")
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$recommendationsTable SET $recommendedItemIdsCol = '$recommendedIDsString', $timestampCol = $latestTimestamp WHERE $userIdCol = $userId")
    }
  }
}


/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val source: Source,
            val sink: Sink,
            val params: Map[String, Any])(implicit val sparkSession: SparkSession) extends StaticConfig {
  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val trainingRatingsDF = source.getTrainingRatings.select(userIdCol, itemIdCol, ratingCol, timestampCol)
    val itemIDsDF = trainingRatingsDF.select(col(itemIdCol)).distinct()
    val userIds = source.getUserIds

    val minPositiveRatingOption = params.get("cf_min_positive_rating").map(_.toString.toDouble)
    val rank = params("cf_rank").toString.toInt
    val regParam = params("cf_reg_param").toString.toDouble
    val maxIterParam = params("cf_max_iter").toString.toInt

    val als = new ALS()
      .setMaxIter(maxIterParam)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol(userIdCol)
      .setItemCol(itemIdCol)
      .setRatingCol(ratingCol)

    val model = als.fit(trainingRatingsDF)

    for(userId <- userIds) {
     val (notRatedPairsDF, latestTimestamp) = getUserItemPairsToRate(userId, trainingRatingsDF, itemIDsDF)
      val predictedRatingsDF = model.transform(notRatedPairsDF)
        .filter(col(predictionCol).isNotNull)
        .select(userIdCol, itemIdCol, predictionCol)

      val filteredPredictedRatings = minPositiveRatingOption match {
        case Some(minPositiveRating) => predictedRatingsDF.filter(col(predictionCol) > minPositiveRating)
        case None => predictedRatingsDF
      }

      val recommendedItems = filteredPredictedRatings.orderBy(col(predictionCol).desc)
        .select(itemIdCol).collect().map(r => r.getInt(0)).toList

      sink.storeRecommendedItemIDs(userId, recommendedItems, latestTimestamp)
    }
  }

  /**
    * @return DataFrame of itemIds and userIds for rating (required by CF job)
    */
  private def getUserItemPairsToRate(userId: Int, ratingsDF: DataFrame, itemIdsDF: DataFrame): (DataFrame, Long) = {
    val itemIdsNotToIncludeDF = ratingsDF.filter(col(userIdCol) === userId).select(itemIdCol, timestampCol) // 4 secs
    val latestTimestamp: Long = itemIdsNotToIncludeDF.agg(max(col(timestampCol))).first().getLong(0)
    val itemIdsNotToIncludeSet = itemIdsNotToIncludeDF.collect()
      .map(r => r.getInt(0))
      .toSet.toList
    val itemsIdsToRate = itemIdsDF.filter(!col(itemIdCol).isin(itemIdsNotToIncludeSet: _*)) // quick
    val notRatedPairsDF = itemsIdsToRate.withColumn(userIdCol, lit(userId))
      .select(col(itemIdCol), col(userIdCol))
    (notRatedPairsDF, latestTimestamp)
  }

//  private def getUserItemPairsToRate1(userIdsDF: DataFrame, ratingsDF: DataFrame, itemIdsDF: DataFrame): (DataFrame, Long) = {
//
//    val itemIdsNotToIncludeDF = ratingsDF.filter(col(userIdCol) === userId).select(itemIdCol, timestampCol) // 4 secs
//    val latestTimestamp: Long = itemIdsNotToIncludeDF.agg(max(col(timestampCol))).first().getLong(0)
//    val itemIdsNotToIncludeSet = itemIdsNotToIncludeDF.collect()
//      .map(r => r.getInt(0))
//      .toSet.toList
//    val itemsIdsToRate = itemIdsDF.filter(!col(itemIdCol).isin(itemIdsNotToIncludeSet: _*)) // quick
//    val notRatedPairsDF = itemsIdsToRate.withColumn(userIdCol, lit(userId))
//      .select(col(itemIdCol), col(userIdCol))
//    (notRatedPairsDF, latestTimestamp)
//  }
}

object CFJob extends {

  val defaultALSParameters = Map(
    "cf_rank"-> 5,
    "cf_max_iter" -> 2,
    "cf_reg_param" -> 1.0
  )

  def apply(config: Config, alsParameters: Map[String, Any] = defaultALSParameters)(implicit sparkSession: SparkSession): CFJob = {

    val keyspace: String = config.getString("cassandra.keyspace")
    val ratingsTable: String = config.getString("cassandra.ratings_table")
    val recommendationsTable: String = config.getString("cassandra.recommendations_table")

    val source = new CassandraSource(keyspace, ratingsTable, None)
    val sink = new CassandraSink(keyspace, recommendationsTable)

    new CFJob(source, sink, defaultALSParameters)
  }
}
