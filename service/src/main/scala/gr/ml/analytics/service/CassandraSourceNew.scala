package gr.ml.analytics.service

import com.datastax.driver.core.Row
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.parsing.json.JSON

class CassandraSourceNew(val config: Config,
                         val featureExtractor: FeatureExtractor)(implicit val sparkSession: SparkSession) extends SourceNew {

  private val schemaId: Int = config.getInt("items_schema_id")

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val ratingsTimestampTable: String = config.getString("cassandra.ratings_timestamp_table")
  private val itemsNotRatedByUserTable: String = config.getString("cassandra.items_not_rated_by_user_table")
  private val itemsTable: String = config.getString("cassandra.items_table_prefix") + schemaId
  private val schemasTable: String = config.getString("cassandra.schemas_table")
  private val notRatedItemsWithFeaturesTable: String = config.getString("cassandra.not_rated_items_with_features_table")

  private val keyCol = "key"
  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val itemIdsCol = "itemids"
  private val ratingCol = "rating"
  private val timestampCol = "timestamp"
  private val predictionCol = "prediction"
  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  private lazy val userIDsDS = spark // TODO do we need this?
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> itemsTable, "keyspace" -> keyspace))
    .load()
    .select(itemIdCol)

  private lazy val itemIDsDS = getAllRatings(ratingsTable) // TODO do we need this?
    .select(col(itemIdCol))
    .distinct()

  override def getPredictionsForUser(userId: Int, table: String): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()
      .select(keyCol, userIdCol, itemIdCol, predictionCol)
    .where(col(userIdCol) === userId)
  }

  private lazy val schema: Map[String, Any] = CassandraConnector(sparkSession.sparkContext).withSessionDo[Map[String, Any]] { session =>

    def convertJson(itemString: String): Map[String, Any] = {
      val json = JSON.parseFull(itemString)
      json match {
        case Some(item: Map[String, Any]) => item
        case None => throw new RuntimeException("item validation error")
      }
    }

    val res = session.execute(s"SELECT JSON * FROM $keyspace.$schemasTable WHERE schemaId = $schemaId").one()
    val json = convertJson(res.get("[json]", classOf[String]))
    convertJson(json("jsonschema").asInstanceOf[String])
  }

  override def getAllRatings(tableName: String): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspace))
      .load()
  }


  override def getUserIdsForLastNSeconds(seconds: Int): Set[Int] = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> ratingsTimestampTable, "keyspace" -> keyspace))
      .load()
      .where(col("timestamp") > (System.currentTimeMillis()/1000 - seconds).toInt)
      .select(userIdCol)
      .collect
      .map(r => r.getInt(0)).toSet
  }

 override def getNotRatedItemsWithFeaturesMap(userId: Int): Map[Int, List[Double]] ={
    val result = spark.read // TODO don't actually need spark to read this! (check if it is faster without spark)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> notRatedItemsWithFeaturesTable, "keyspace" -> keyspace))
      .load()
      .where(col("userid") === 808) // TODO unhardcode!
      .select("items")
      .collect()

    if(result.size > 0)
      result(0).getMap(0).asInstanceOf[Map[Int, List[Double]]]
    else
      Map()
  }

  override def getNotRatedItems(userId: Int): DataFrame = {
    getNotRatedItemsWithFeaturesMap(userId)
      .keys.toList.toDF("itemId")
      .withColumn("userId", lit(userId))


    // TODO nothing wrong with this code, I just want to try to use single cassandra table for both CF and CB
//    val itemIds: List[Int] = spark.read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> itemsNotRatedByUserTable, "keyspace" -> keyspace))
//      .load()
//      .where(col("userid") === userId)
//      .select(itemIdsCol)
//      .collect()
//      .toList(0).getList(0)
//      .toArray.toList
//      .asInstanceOf[List[Int]]
//
//    itemIds.toDF("itemId")
//      .withColumn("userId", lit(userId))
  }

  override def getNotRatedItemsWithFeatures(userId: Int): DataFrame = {
    getNotRatedItemsWithFeaturesMap(userId)
      .map(t => (t._1, Vectors.dense(t._2.toArray)))
      .toList.toDF("itemId", "features")
      .withColumn("userId", lit(userId))
  }


  override def getAllItemsAndFeatures(): DataFrame = {

    val itemsRowDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> itemsTable, "keyspace" -> keyspace))
      .load()

    featureExtractor.convertFeatures(itemsRowDF, schema)
  }
}
