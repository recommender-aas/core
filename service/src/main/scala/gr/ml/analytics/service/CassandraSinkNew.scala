package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import gr.ml.analytics.util.{ParamsStorage, RedisParamsStorage}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col
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

  private val cfPredictionsColumn: String = config.getString("cassandra.cf_predictions_column")
  private val cbPredictionsColumn: String = config.getString("cassandra.cb_predictions_column")
  private val hybridPredictionsColumn: String = config.getString("cassandra.hybrid_predictions_column")

  val paramsStorage: ParamsStorage = new RedisParamsStorage

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$predictionsTable (userid int, itemid int, cf_prediction float, cb_prediction float, hybrid_prediction float, primary key (userid, itemid))")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
//    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable (userid int PRIMARY KEY, recommended_ids text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$trainRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$itemClustersTable (itemid int PRIMARY KEY, similar_items text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$testRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float, timestamp int)")
  }

    override def clearTable(table: String): Unit = {
      CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
        session.execute(s"TRUNCATE $keyspace.$table")
      }
    }
    // todo rename to update predictions:
    override def updatePredictions(userId: Int, itemId: Int, predictedValue: Float, predictionColumn: String): Unit = {
      CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>

        val getPredictionsResult = session.execute(s"SELECT cf_prediction, cb_prediction FROM $keyspace.$predictionsTable " +
          s"WHERE userid = $userId AND itemid = $itemId").one() // TODO it is quick. probably can use it elsewhere too instead of spark.

        var cfPrediction = 0.0
        var cbPrediction = 0.0

        if(getPredictionsResult != null){
          cfPrediction = getPredictionsResult.getFloat(0)
          cbPrediction = getPredictionsResult.getFloat(1)
        }

        if (predictionColumn == cfPredictionsColumn)
          cfPrediction = predictedValue
        if (predictionColumn == cbPredictionsColumn)
          cbPrediction = predictedValue
        val cfWeight = paramsStorage.getParam("hb_collaborative_weight").toString.toDouble

        var hybridPrediction = 0.0
        if (cfPrediction != 0.0 && cbPrediction != 0.0)
          hybridPrediction = cfPrediction * cfWeight + cbPrediction * (1 - cfWeight)
        else
          hybridPrediction = cfPrediction + cbPrediction

        session.execute(s"UPDATE $keyspace.$predictionsTable " +
          s"SET cf_prediction = $cfPrediction, cb_prediction = $cbPrediction, hybrid_prediction = $hybridPrediction" +
          s" where userid = $userId and itemid = $itemId")
      }
    }

  override def storeRecommendedItemIDs(userId: Int): Unit = {
    val itemIDs = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> predictionsTable, "keyspace" -> keyspace))
      .load()
      .where(col("userid") === userId)
      .select("itemid", hybridPredictionsColumn)
      .orderBy(col(hybridPredictionsColumn))
      .select("itemid")
      .collect
      .map(i => i.getInt(0))

    val itemIDsString = "[" + itemIDs.mkString(", ") + "]"

    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$recommendationsTable SET itemids = $itemIDsString WHERE userid = $userId")
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
