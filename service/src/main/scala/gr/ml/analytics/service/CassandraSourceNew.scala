package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class CassandraSourceNew(val config: Config,
                         val featureExtractor: FeatureExtractor)(implicit val sparkSession: SparkSession) extends SourceNew {

  private val schemaId: Int = config.getInt("items_schema_id")

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val ratingsTimestampTable: String = config.getString("cassandra.ratings_timestamp_table")
  private val itemsTable: String = config.getString("cassandra.items_table_prefix") + schemaId
  private val schemasTable: String = config.getString("cassandra.schemas_table")
  private val notRatedItemsWithFeaturesTable: String = config.getString("cassandra.not_rated_items_with_features_table")
  private val predictionsTable: String = config.getString("cassandra.predictions_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  val asDense = udf((array: scala.collection.mutable.WrappedArray[Double]) => Vectors.dense(array.toArray))

  override def getPredictionsForUser(userId: Int, predictionColumn: String): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> predictionsTable, "keyspace" -> keyspace))
      .load()
      .select(userIdCol, itemIdCol, predictionColumn)
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
      .withColumn("features_vector", asDense(col("features")))
      .select(col("userid").as("userId"), col("itemid").as("itemId"), col("rating"), col("features_vector").as("features"))
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

 def getNotRatedItemsWithFeaturesMap(userId: Int): Map[Int, List[Double]] ={
    val result = spark.read // TODO don't actually need spark to read this! (check if it is faster without spark)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> notRatedItemsWithFeaturesTable, "keyspace" -> keyspace))
      .load()
      .where(col("userid") === userId)
      .select("items")
      .collect()

    if(result.size > 0)
      result(0).getMap(0).asInstanceOf[Map[Int, List[Double]]]
    else
      Map()
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
