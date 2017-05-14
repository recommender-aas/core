package gr.ml.analytics

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraConnector, CassandraStorage}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.util.{Failure, Success}


/**
  * Command line tool used to download movielens dataset
  * to local file system and upload ratings from it to the recommender service
  * via REST client.
  *
  * See https://grouplens.org/datasets/movielens
  */
object CassandraWriterTest extends App with LazyLogging {

  val config = ConfigFactory.load("application.conf")
  val cassandraHosts: List[String] = config.getStringList("cassandra.host").toList
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraUsername: String = config.getString("cassandra.username")
  val cassandraPassword: String = config.getString("cassandra.password")

  def cassandraConnector = CassandraConnector(
    cassandraHosts,
    cassandraKeyspace,
    Some(cassandraUsername),
    Some(cassandraPassword))

  val inputDatabase = new CassandraStorage(cassandraConnector.connector)


  var userId = 1
  var itemId = 1
  val rating = 3.5
  while(userId <= 100000){
    val query2 = s"INSERT INTO rs_keyspace.ratings (userid, itemid, rating) values ($userId, $itemId, $rating)";
    val wasApplied2 = inputDatabase.connector.session.execute(query2).wasApplied()
    println("Saved into ratings table - " + wasApplied2)
    userId = userId + 1
  }

  println("Hello!")



}
