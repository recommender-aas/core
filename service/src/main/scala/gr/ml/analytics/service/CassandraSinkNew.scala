package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraSinkNew(val config: Config)
                      (implicit val sparkSession: SparkSession) extends SinkNew {

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val predictionsTable: String = config.getString("cassandra.predictions_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")
  private val recommendationsTable: String = config.getString("cassandra.recommendations_table")
  private val trainRatingsTable: String = config.getString("cassandra.train_ratings_table")
  private val testRatingsTable: String = config.getString("cassandra.test_ratings_table")
  private val itemClustersTable: String = config.getString("cassandra.item_clusters_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$predictionsTable (userid int, itemid int, cf_prediction float, cb_prediction float, hybrid_prediction float, primary key (userid, itemid))")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable (userid int PRIMARY KEY, recommended_ids text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$trainRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$itemClustersTable (itemid int PRIMARY KEY, similar_items text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$testRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float, timestamp int)")
  }

  override def clearTable(table: String): Unit = {
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"TRUNCATE $keyspace.$table")
    }
  }

  override def storePrediction(userId: Int, itemId: Int, predictedValue: Float, predictionColumn: String): Unit = {
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$predictionsTable " +
        s"SET $predictionColumn = $predictedValue where userid = $userId and itemid = $itemId")
    }
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
