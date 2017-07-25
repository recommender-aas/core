package gr.ml.analytics.batch.cf

import java.io.IOException

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.FunSuite

class CFJobTest extends FunSuite with StaticConfig {

  EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, 5000L)
  val port: Int = EmbeddedCassandraServerHelper.getNativeTransportPort
  val host: String = EmbeddedCassandraServerHelper.getHost

  val sparkConf = new SparkConf()
    .set("spark.cassandra.connection.host", host)
    .set("spark.cassandra.connection.port", port.toString)
//    .set("spark.sql.crossJoin.enabled", "true")

  implicit val sparkSession: SparkSession = getSparkSession(sparkConf)

  val keyspace = "test"
  val ratingsTable = "ratings"
  val recommendationsTable = "predictions"

  val config = ConfigFactory.parseString(
    s"""
       |cassandra.keyspace = "$keyspace"
       |cassandra.ratings_table = "$ratingsTable"
       |cassandra.recommendations_table = "$recommendationsTable"
    """.stripMargin)

  val alsParams = Map(
    "cf_rank"-> 5,
    "cf_max_iter" -> 20,
    "cf_reg_param" -> 1.0
  )

  def testUserPredicateFunction(ratingsDF: DataFrame): Set[String] = Set("1a", "7a", "8a", "10a")

  val job = CFJob(config, testUserPredicateFunction, alsParams)

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$ratingsTable ($ratingsKeyCol varchar PRIMARY KEY, $userIdCol varchar, $itemIdCol varchar, $ratingCol float, $timestampCol bigint)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable ($userIdCol varchar PRIMARY KEY, $recommendedItemIdsCol text, $timestampCol bigint)")
  }


  test("collaborative filtering should provide valid recommendations") {
    val ratingsDF = sparkSession.read
      .option("header", "true")
      .csv(getFileWithUtil("ratings.csv").get)
      .select(
        col(userIdCol).cast(StringType),
        col(itemIdCol).cast(StringType),
        col(ratingCol).cast(DoubleType),
        col(timestampCol).cast(LongType))
      .withColumn(ratingsKeyCol, concat(col(userIdCol), lit(":"), col(itemIdCol)))

    ratingsDF.select(ratingsKeyCol, userIdCol, itemIdCol, ratingCol, timestampCol)
      .write.mode("overwrite")
      .cassandraFormat(ratingsTable, keyspace)
      .save()

    // check stored ratings size in cassandra
    val storedSize = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
      .load()
      .select(ratingsKeyCol, userIdCol, itemIdCol, ratingCol, timestampCol).collect().length
    assert(storedSize === 79)

    val elapsedSec = TestUtil.timed(() => job.run())

    // check recommendations
    val recommendationsDF = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> recommendationsTable, "keyspace" -> keyspace))
      .load()
      .select(userIdCol, recommendedItemIdsCol, timestampCol)

    recommendationsDF.show(100)

    val recommendations: Map[String, (List[String], Long)] = recommendationsDF.collect
      .map(row => (row.getAs[String](userIdCol), (row.getAs[String](recommendedItemIdsCol).split(":").toList, row.getAs[Long](timestampCol))))
      .toMap

    val expectedResult = Map(
      "1a" -> (List("11b", "9b", "10b", "12b", "14b", "15b", "13b"), 1000000007),
      "7a" -> (List("1b", "4b", "6b", "5b", "15b", "3b", "2b"), 1000000055),
      "8a" -> (List("7b", "1b", "6b", "4b", "5b", "3b", "2b"), 1000000063),
      "10a" -> (List("6b", "10b", "9b", "5b", "3b", "2b"), 1000000078)
    )

    expectedResult.foreach(pair => {
      val act = recommendations(pair._1)
      val exp = pair._2
      assert(act === exp)
    })


    TestUtil.printElapsedSec(elapsedSec)
  }

  private def getSparkSession(sparkConf: SparkConf): SparkSession = {
    SparkSession.builder().config(sparkConf)
      .master("local[*]")
      .appName("Spark Recommendation Service")
      .getOrCreate
  }

  private def getFileWithUtil(fileName: String): Option[String] = {
    try
      Some(getClass.getClassLoader.getResource(fileName).toString)
    catch {
      case e: IOException =>
        e.printStackTrace()
        None
    }
  }
}
