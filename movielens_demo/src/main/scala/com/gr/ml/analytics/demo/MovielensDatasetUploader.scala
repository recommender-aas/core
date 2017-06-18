package com.gr.ml.analytics.demo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.github.tototoshi.csv.CSVReader
import com.gr.ml.analytics.demo.extractor.GenresFeatureEngineering
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.{JSON, JSONArray, JSONObject}
import scala.util.{Failure, Success}


/**
  * Command line tool used to download movielens dataset
  * to local file system and upload ratings from it to the recommender service
  * via REST client.
  *
  * See https://grouplens.org/datasets/movielens
  */
object MovielensDatasetUploader extends App with Constants with LazyLogging {

  Util.loadAndUnzip()     // TODO it should perform feature generation too!

  val ratingMapping = Map(
    0.5 -> 1,
    1 -> 2,
    1.5 -> 3,
    2 -> 4,
    2.5 -> 5,
    3 -> 6,
    3.5 -> 7,
    4 -> 8,
    4.5 -> 9,
    5 -> 10
  )

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val config: Config = ConfigFactory.load("application.conf")

  val serviceREST = config.getString("service.rest")


  def escapedFeatureName(featureName: String): String = featureName.toLowerCase.replaceAll("[^a-zA-Z]", "")

  val featureNames = CSVReader.open(moviesWithFeaturesPath).readNext().get.drop(1)
  val featureNumbers = featureNames.indices.toList
  val featuresMap = (featureNumbers zip featureNames).toMap


  /**
    * Creates schema for item format.
    *
    * @return the id of created schema
    */
  def postSchema(): String = {

    // the first column is itemId
    val numFeatures = CSVReader.open(moviesWithFeaturesPath).readNext().get.length - 1
    val schema = JSONObject(Map(
      "id" -> JSONObject(Map(
        "name" -> "movieid",
        "type" -> "int"
      )),
      "features" -> JSONArray(featureNames.map(featureName =>
        JSONObject(Map(
          "name" -> escapedFeatureName(featureName),
          "type" -> "double"
        ))
      )
    )))

    val future = Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = s"$serviceREST/schemas",
      entity = HttpEntity(ContentTypes.`application/json`, schema.toString())))

    future.onFailure { case e => e.printStackTrace() }
    Await.ready(future, 10.seconds)
    future.value.get match {
      case Success(response) =>

        def convertJson(schemaString: String): Map[String, Any] = {
          val json = JSON.parseFull(schemaString)
          json match {
            case Some(schema: Map[String, Any]) => schema
            case None => throw new RuntimeException("item validation error")
          }
        }

        val responseString = response.entity.asInstanceOf[HttpEntity.Strict].getData().decodeString(ByteString.UTF_8).toString
        convertJson(responseString)("id").toString
      case Failure(ex) => throw ex
    }
  }

  def uploadMovies(schemaId: String): Unit = {

    val reader = CSVReader.open(moviesWithFeaturesPath)

    val allItems = reader.toStreamWithHeaders.map(map => {
      val fMap: Map[String, Any] = featuresMap.map(
        entry => (escapedFeatureName(entry._2.toString), map(entry._2).toDouble)
      ) + ("movieid" -> map("itemId").toInt)
      JSONObject(fMap)
    }).toList

    def postItem(movieList: List[JSONObject]): Future[HttpResponse] = {

      def toJson(movieList: List[JSONObject]): String = {
        JSONArray(movieList).toString()
      }

      val future = Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = s"$serviceREST/schemas/$schemaId/items",
        entity = HttpEntity(ContentTypes.`application/json`, toJson(movieList))))

      future.onFailure { case e => e.printStackTrace() }
      future
    }

    // every request will contain 1000 movies
    val groupedMovies: List[List[JSONObject]] = allItems.grouped(500).toList

    groupedMovies.foreach(movies => {
      val future = postItem(movies)
      Await.ready(future, 30.seconds)
      println(future.value.get)
      Thread.sleep(2000)
    })

  }

  case class Rating(userId: Int, itemId: Int, rating: Double, timestamp: Long)

  def uploadRatings(): Unit = {

    val reader = CSVReader.open(ratingsPath)

    def postRating(ratingList: List[Rating]): Future[HttpResponse] = {

      def toJson(ratingList: List[Rating]): String = {
        JSONArray(ratingList.map(rating => JSONObject(Map(
          "userId" -> rating.userId,
          "itemId" -> rating.itemId,
          "rating" -> rating.rating/*,
          "timestamp" -> rating.timestamp*/)))).toString()
      }

      val future = Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = s"$serviceREST/ratings",
        entity = HttpEntity(ContentTypes.`application/json`, toJson(ratingList))))

      future.onFailure { case e => e.printStackTrace() }
      future
    }

    val allRatings = reader.toStreamWithHeaders.flatMap(map => {
      for {
        userId <- map.get("userId")
        itemId <- map.get("movieId")
        rating <- map.get("rating")
        timestamp <- map.get("timestamp")
      } yield Rating(userId.toInt, itemId.toInt, rating.toDouble, timestamp.toLong)
    }).toList

    // every request will contain 1000 ratings
    val groupedMovies: List[List[Rating]] = allRatings.grouped(1000).toList

    groupedMovies.foreach(ratings => {
      val future = postRating(ratings)
      Await.ready(future, 30.seconds)
      println(future.value.get)
    })



  }

  logger.info("Creating schema")

  val schemaId = postSchema()
  logger.info("Uploading items corresponding to schema")
  uploadMovies(schemaId)

  logger.info("Uploading ratings")
  uploadRatings()

  system.terminate()
}
