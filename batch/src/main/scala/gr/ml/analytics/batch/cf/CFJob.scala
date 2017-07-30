package gr.ml.analytics.batch.cf

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


trait StaticConfig {
  val userIdCol = "userid"
  val itemIdCol = "itemid"
  val userIdNumCol = "userid_num"
  val itemIdNumCol = "itemid_num"
  val ratingCol = "rating"
  val timestampCol = "timestamp"
  val predictionCol = "prediction"
  val recommendedItemIdsCol = "recommended_item_ids"
  val ratingsKeyCol = "key"
}


trait Source {
  /**
    * @return DataFrame of (userId: String, itemId: String, rating: float, timestamp: long) to train model
    */
  def getTrainingRatings: DataFrame

  /**
    * @return Set of userIds the performed latest ratings
    */
  def getUserIds(userPredicateFunc: (DataFrame) => Set[String]): Set[String]
}


trait Sink {
  def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int], latestTimestamp: Long)

  /**
    * @param predictionsDF DataFrame of (userId: String, recommended_item_ids: String, timestamp: long)
    *                      where timestamp is the time of latest user rating used by current model.
    */
  def storeRecommendedItemIDs(predictionsDF: DataFrame)
}


class CassandraSource(val keyspace: String,
                      val ratingsTable: String,
                      val startPredictionTimestamp: Option[Long])(implicit val sparkSession: SparkSession) extends Source with StaticConfig {

  private lazy val ratingsDF = sparkSession.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
    .load()
    .select(ratingsKeyCol, userIdCol, itemIdCol, ratingCol, timestampCol)
    .cache()

  override def getTrainingRatings: DataFrame = ratingsDF

  override def getUserIds(userPredicateFunc: (DataFrame) => Set[String]): Set[String] = userPredicateFunc(ratingsDF)
}


class CassandraSink(val keyspace: String,
                    val recommendationsTable: String)(implicit val sparkSession: SparkSession) extends Sink with StaticConfig {

  override def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int], latestTimestamp: Long): Unit = {
    val recommendedIDsString = recommendedItemIds.toArray.mkString(":")
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$recommendationsTable SET $recommendedItemIdsCol = '$recommendedIDsString', $timestampCol = $latestTimestamp WHERE $userIdCol = $userId")
    }
  }

  override def storeRecommendedItemIDs(predictionsDF: DataFrame): Unit = {
    predictionsDF.select(userIdCol, recommendedItemIdsCol, timestampCol)
      .write.mode("append")
      .cassandraFormat(recommendationsTable, keyspace)
      .save()
  }
}


/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val source: Source,
            val sink: Sink,
            val userPredicateFunc: (DataFrame) => Set[String],
            val params: Map[String, Any])(implicit val sparkSession: SparkSession) extends StaticConfig {

  import sparkSession.implicits._

  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val trainingRatingsDF = source.getTrainingRatings.select(ratingsKeyCol, userIdCol, itemIdCol, ratingCol, timestampCol)

    val userIndexer = new StringIndexer()
      .setInputCol(userIdCol)
      .setOutputCol(userIdNumCol)
    val itemIndexer = new StringIndexer()
      .setInputCol(itemIdCol)
      .setOutputCol(itemIdNumCol)

    val userIndexerModel = userIndexer.fit(trainingRatingsDF)
    val itemIndexerModel = itemIndexer.fit(trainingRatingsDF)

    val trainingRatings1DF = userIndexerModel.transform(trainingRatingsDF)
    val trainingRatings2DF = itemIndexerModel.transform(trainingRatings1DF)

    val itemIDsDF = trainingRatings2DF.select(col(itemIdNumCol), col(itemIdCol)).distinct()
    val userIds = source.getUserIds(userPredicateFunc)

    val minPositiveRatingOption = params.get("cf_min_positive_rating").map(_.toString.toDouble)
    val rank = params("cf_rank").toString.toInt
    val regParam = params("cf_reg_param").toString.toDouble
    val maxIterParam = params("cf_max_iter").toString.toInt

    val als = new ALS()
      .setMaxIter(maxIterParam)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol(userIdNumCol)
      .setItemCol(itemIdNumCol)
      .setRatingCol(ratingCol)

    val model = als.fit(trainingRatings2DF)

//    val recDF = model.recommendForAllUsers(10)
//    recDF.show(100)
//
//    val userIdMap = userIDsDF.collect().map(row => (row.getAs[Int](userIdNumCol), row.getAs[String](userIdCol))).toMap
//    val itemIdMap = itemIDsDF.collect().map(row => (row.getAs[Int](itemIdNumCol), row.getAs[String](itemIdCol))).toMap
//
//    val recFinalDF = recDF.map(row => {
//      val uIdNum = row.getAs[Int](userIdNumCol)
//      val recNum = row.getAs[mutable.WrappedArray[Array[Object]]]("recommendations")
//      (userIdMap(uIdNum))
//    })

    val userIdDF = userIndexerModel.transform(userIds.toList.toDF(userIdCol))
      .select(col(userIdCol), col(userIdNumCol))

    val usersRatingsDF = trainingRatings2DF
      .select(userIdNumCol, itemIdNumCol, userIdCol, itemIdCol, ratingCol, timestampCol, ratingsKeyCol)
      .as("d1").join(userIdDF.as("d2"), $"d1.$userIdNumCol" === $"d2.$userIdNumCol")
      .select($"d1.$itemIdNumCol", $"d1.$userIdNumCol", $"d1.$userIdCol", $"d1.$itemIdCol", $"d1.$ratingCol", $"d1.$timestampCol", $"d1.$ratingsKeyCol")

    val userLatestRatingTimestampDF = usersRatingsDF.groupBy(userIdCol).agg(max(timestampCol).as(timestampCol))

    val userNotRatedItemsDF = userIdDF
      .crossJoin(itemIDsDF)
      .withColumn(ratingsKeyCol, concat(col(userIdCol), lit(":"), col(itemIdCol)))
      .select(userIdNumCol, itemIdNumCol, userIdCol, itemIdCol, ratingsKeyCol)
      .except(usersRatingsDF.select(userIdNumCol, itemIdNumCol, userIdCol, itemIdCol, ratingsKeyCol))
      //TODO some bug itemIdNumCol is of StringType so cust
      .select(
        col(userIdNumCol).cast(DoubleType),
        col(itemIdNumCol).cast(DoubleType),
        col(userIdCol),
        col(itemIdCol),
        col(ratingsKeyCol)
      )

    val predictedDF = model.transform(userNotRatedItemsDF)
      .filter(col(predictionCol).isNotNull)
      .select(ratingsKeyCol, userIdNumCol, itemIdNumCol, userIdCol, itemIdCol, predictionCol)

    val filteredPredictedDF = minPositiveRatingOption match {
      case Some(minPositiveRating) => predictedDF.filter(col(predictionCol) > minPositiveRating)
      case None => predictedDF
    }

    val aggregatedDF = filteredPredictedDF
      .sort(col(userIdCol), col(predictionCol).desc)
      .groupBy(userIdCol)
      .agg(concat_ws(":", collect_list(itemIdCol)).as(recommendedItemIdsCol))

    val resultDF = aggregatedDF.as("d1").join(userLatestRatingTimestampDF.as("d2"), $"d1.$userIdCol" === $"d2.$userIdCol")
      .select($"d1.$userIdCol", $"d1.$recommendedItemIdsCol", $"d2.$timestampCol")

    sink.storeRecommendedItemIDs(resultDF)

//    for(userId <- userIds) {
    //     val (notRatedPairsDF, latestTimestamp) = getUserItemPairsToRate(userId, trainingRatingsDF, itemIDsDF)
    //      val predictedRatingsDF = model.transform(notRatedPairsDF)
    //        .filter(col(predictionCol).isNotNull)
    //        .select(userIdCol, itemIdCol, predictionCol)
    //
    //      val filteredPredictedRatings = minPositiveRatingOption match {
    //        case Some(minPositiveRating) => predictedRatingsDF.filter(col(predictionCol) > minPositiveRating)
    //        case None => predictedRatingsDF
    //      }
    //
    //      val recommendedItems = filteredPredictedRatings.orderBy(col(predictionCol).desc)
    //        .select(itemIdCol).collect().map(r => r.getInt(0)).toList
    //
    //      sink.storeRecommendedItemIDs(userId, recommendedItems, latestTimestamp)
    //    }
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
}

object CFJob extends StaticConfig {

  val defaultALSParameters = Map(
    "cf_rank"-> 5,
    "cf_max_iter" -> 2,
    "cf_reg_param" -> 1.0
  )

  def timestampUserPredicateFunc(timestampOpt: Option[Long])(ratingsDF: DataFrame): Set[String] = {
    val userIdsDF = timestampOpt match {
      case Some(timestamp) =>
        ratingsDF.filter(col(timestampCol) > timestamp)
          .select(userIdCol).distinct()
      case None =>
        ratingsDF.select(userIdCol).distinct()
    }
    userIdsDF.collect().map(r => r.getString(0)).toSet
  }

  def apply(config: Config,
            userPredicateFunc: (DataFrame) => Set[String] = timestampUserPredicateFunc(None),
            alsParameters: Map[String, Any] = defaultALSParameters)(implicit sparkSession: SparkSession): CFJob = {

    val keyspace: String = config.getString("cassandra.keyspace")
    val ratingsTable: String = config.getString("cassandra.ratings_table")
    val recommendationsTable: String = config.getString("cassandra.recommendations_table")

    val source = new CassandraSource(keyspace, ratingsTable, None)
    val sink = new CassandraSink(keyspace, recommendationsTable)

    new CFJob(source, sink, userPredicateFunc, defaultALSParameters)
  }
}
