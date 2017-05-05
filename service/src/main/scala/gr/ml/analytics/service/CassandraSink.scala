package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

class CassandraSink(val config: Config)
                   (implicit val sparkSession: SparkSession) extends Sink {

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")
  private val cbPredictionsTable: String = config.getString("cassandra.cb_predictions_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")
  private val hybridPredictionsTable: String = config.getString("cassandra.hybrid_predictions_table")
  private val recommendationsTable: String = config.getString("cassandra.recommendations_table")
  private val trainRatingsTable: String = config.getString("cassandra.train_ratings_table")
  private val testRatingsTable: String = config.getString("cassandra.test_ratings_table")
  private val itemClustersTable: String = config.getString("cassandra.item_clusters_table")
  private val notRatedItemsWithFeaturesTable: String = config.getString("cassandra.not_rated_items_with_features_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cfPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cbPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$hybridPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable (userid int PRIMARY KEY, recommended_ids text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$trainRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$itemClustersTable (itemid int PRIMARY KEY, similar_items text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$testRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float, timestamp int)")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$notRatedItemsWithFeaturesTable (userid int PRIMARY KEY, items map<int, frozen<list<double>>>)")
  }

  override def clearTable(table: String): Unit = {
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"TRUNCATE $keyspace.$table")
    }
  }

  override def storePredictions(predictions: DataFrame, predictionsTable: String): Unit = {
    predictions
      .select(col("userId").as("userid"), col("itemId").as("itemid"), col("prediction"))
      .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
      .write.mode("append")
      .cassandraFormat(predictionsTable, keyspace)
      .save()
  }

  override def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int]): Unit = {
    val recommendedIDsString = recommendedItemIds.toArray.mkString(":")
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$recommendationsTable SET recommended_ids = '$recommendedIDsString' WHERE userid = $userId")
    }
  }

  override def storeItemClusters(itemClustersDF: DataFrame): Unit = {
    itemClustersDF
      .select("itemid", "similar_items")
      .write.mode("append")
      .cassandraFormat(itemClustersTable, keyspace)
      .save()
  }
}
