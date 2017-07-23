package gr.ml.analytics.batch.cf

import java.io.IOException

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.FunSuite

class CFJobTest extends FunSuite with StaticConfig {

  EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, 5000L)
  val port: Int = EmbeddedCassandraServerHelper.getNativeTransportPort
  val host: String = EmbeddedCassandraServerHelper.getHost

  val sparkConf = new SparkConf()
    .set("spark.cassandra.connection.host", host)
    .set("spark.cassandra.connection.port", port.toString)

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

  val job = CFJob(config, alsParams)

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$ratingsTable (key text PRIMARY KEY, $userIdCol int, $itemIdCol int, $ratingCol float, $timestampCol bigint)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable ($userIdCol int PRIMARY KEY, $recommendedItemIdsCol text, $timestampCol bigint)")
  }


  test("collaborative filtering should provide valid recommendations") {
    val ratingsDF = sparkSession.read
      .option("header", "true")
      .csv(getFileWithUtil("ratings.csv").get)
      .select(
        col(userIdCol).cast(IntegerType),
        col(itemIdCol).cast(IntegerType),
        col(ratingCol).cast(DoubleType),
        col(timestampCol).cast(LongType))
      .withColumn("key", concat(col(userIdCol), lit(":"), col(itemIdCol)))

    ratingsDF.select("key", userIdCol, itemIdCol, ratingCol, timestampCol)
      .write.mode("overwrite")
      .cassandraFormat(ratingsTable, keyspace)
      .save()

    // check stored ratings size in cassandra
    val storedSize = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
      .load()
      .select(userIdCol, itemIdCol, ratingCol, timestampCol).collect().length
    assert(storedSize === 79)

    val elapsedSec = TestUtil.timed(() => job.run())

    // check recommendations
    val recommendationsDF = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> recommendationsTable, "keyspace" -> keyspace))
      .load()
      .select(userIdCol, recommendedItemIdsCol, timestampCol)

    recommendationsDF.show(100)

    val recommendations: Map[Int, (List[Int], Long)] = recommendationsDF.collect
      .map(row => (row.getAs[Int](userIdCol), (row.getAs[String](recommendedItemIdsCol).split(":").map(_.toInt).toList, row.getAs[Long](timestampCol))))
      .toMap

    val expectedResult = Map(
      1 -> (List(11, 9, 10, 12, 14, 15, 13), 1000000007),
      7 -> (List(1, 4, 6, 5, 15, 3, 2), 1000000055),
      8 -> (List(7, 1, 6, 4, 5, 3, 2), 1000000063),
      10 -> (List(6, 10, 9, 5, 3, 2), 1000000078)
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
