package com.gr.ml.analytics.demo

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.github.tototoshi.csv.{CSVFormat, CSVReader, QUOTE_MINIMAL, Quoting}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.util.parsing.json.{JSONArray, JSONObject}


object DatasetFeaturesExtractor extends LazyLogging {

  // ratings settings
  val rawRatingsPath = Set(
    Paths.get("test_dataset", "2013.csv").toAbsolutePath.toString,
    Paths.get("test_dataset", "2014.csv").toAbsolutePath.toString
  )
  val itemIdColName = "\"order_line/product_tmpl_id/id\""
  val userIdColName = "\"partner_id/id\""
  val ratingColName = "\"state\""

  val itemsPath = Set(
    Paths.get("test_dataset", "product.template.csv").toAbsolutePath.toString
  )

  val featureNamePrefix = "f"
  val feature1ColName = "\"categ_ids/.id\""
  val itemsItemIdColName = "\"id\""

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val config: Config = ConfigFactory.load("application.conf")

    val serviceREST = config.getString("service.rest")


    val csvFormat = new CSVFormat {
      val delimiter: Char = ';'
      val quoteChar: Char = '"'
      val escapeChar: Char = '"'
      val lineTerminator: String = "\r\n"
      val quoting: Quoting = QUOTE_MINIMAL
      val treatEmptyLineAsNil: Boolean = false
    }


    def uploadSchemaAndItems(): Unit = {

      itemsPath.foreach(itemPath => {

        var lastId: String = null
        val allItems = CSVReader.open(itemPath)(csvFormat)
          .toStreamWithHeaders
          .filter(map => {
              !map(feature1ColName).replace("\"", "").isEmpty
          })
          .map(map => {
            val itemId = {
              val id = map(itemsItemIdColName).replace("\"", "")
              if (id.isEmpty) {
                lastId
              } else {
                lastId = id
                id
              }
            }

            val itemIdNum = itemId.substring(itemId.lastIndexOf("_") + 1)

            val category = featureNamePrefix + map(feature1ColName).replace("\"", "")

            (itemIdNum, category)
          })
          .groupBy(_._1).map { case (k,v) => (k,v.map(_._2).toList)} // group by key
          .toList

        val allCategories = allItems
          .flatMap(tuple => tuple._2)
          .distinct
          .zipWithIndex.toMap


        val schema = JSONObject(Map(
          "id" -> JSONObject(Map(
            "name" -> "itemid",
            "type" -> "int"
          )),
          "features" -> JSONArray(allCategories.map(tuple =>
            JSONObject(Map(
              "name" -> tuple._1,
              "type" -> "double"
            ))
          ).toList
          ))).toString()

        def postSchema(schema: String): Int = {
          val future = Http().singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = s"$serviceREST/schemas",
            entity = HttpEntity(ContentTypes.`application/json`, schema)))

          future.onFailure { case e => e.printStackTrace() }
          Await.ready(future, 10.seconds)
          future.value.get match {
            case Success(response) =>
              // TODO more elegant way to get Int value from response?
              response.entity.asInstanceOf[HttpEntity.Strict].getData().decodeString(ByteString.UTF_8).toInt
            case Failure(ex) => throw ex
          }
        }

        val schemaId = postSchema(schema)

        val itemJsonObjects = allItems.map(keyAndCategory => {
          val presentFeatures: Map[String, Any] = keyAndCategory._2.map(k => (k, 1.toDouble)).toMap
          val allFeatures: Map[String, Any] = allCategories.map(entry => {
            (entry._1.toString, 0)
          }) + ("itemid" -> keyAndCategory._1.toInt)

          val resMap = allFeatures ++ presentFeatures
          JSONObject(resMap)
        })


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
        val groupedMovies: List[List[JSONObject]] = itemJsonObjects.grouped(1000).toList

        groupedMovies.foreach(movies => {
          val future = postItem(movies)
          Await.ready(future, 30.seconds)
          println(future.value.get)
        })


      })

    }

    case class Rating(userId: Int, itemId: Int, rating: Double)

    def uploadRatings(): Unit = {

      def postRating(ratingList: List[Rating]): Future[HttpResponse] = {

        def toJson(ratingList: List[Rating]): String = {
          JSONArray(ratingList.map(rating => JSONObject(Map(
            "userId" -> rating.userId,
            "itemId" -> rating.itemId,
            "rating" -> rating.rating)))).toString()
        }

        val future = Http().singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = s"$serviceREST/ratings",
          entity = HttpEntity(ContentTypes.`application/json`, toJson(ratingList))))

        future.onFailure { case e => e.printStackTrace() }
        future
      }

      // for every ratings file
      rawRatingsPath.foreach(ratingsFileName => {

        val reader = CSVReader.open(ratingsFileName)(csvFormat)
        val allRatings = reader.toStreamWithHeaders
          .filter(map => {
            // filter empty
            val userId = map(userIdColName).toString.replace("\"", "")
            val itemId = map(itemIdColName).toString.replace("\"", "")
            val rowRating = map(ratingColName).toString.replace("\"", "")
            if (userId.isEmpty || itemId.isEmpty || rowRating.isEmpty) false
            else true
          })
          .map(map => {
            val userId = {
              val userId = map(userIdColName).toString.replace("\"", "")
              userId.substring(userId.lastIndexOf("_") + 1)
            }
            val itemId = {
              val itemId = map(itemIdColName).toString.replace("\"", "")
              itemId.substring(itemId.lastIndexOf("_") + 1)
            }
            val rating = {
              val rowRating = map(ratingColName).toString.replace("\"", "")
              if (rowRating.equals("Sales Order")) {
                4.0
              } else if (rowRating.equals("Done")) {
                5.0
              } else {
                throw new RuntimeException
              }
            }
            Rating(userId.toInt, itemId.toInt, rating.toDouble)
          }).toList

        // every request will contain 1000 ratings
        val groupedMovies: List[List[Rating]] = allRatings.grouped(1000).toList

        groupedMovies.foreach(ratings => {
          val future = postRating(ratings)
          Await.ready(future, 30.seconds)
          println(future.value.get)
        })

      })

    }

    logger.info("Creating schema and items")
    uploadSchemaAndItems()

    logger.info("Uploading ratings")
    uploadRatings()

    system.terminate()
  }
}
