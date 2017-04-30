package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, post}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.RatingService
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import gr.ml.analytics.domain.JsonSerDeImplicits._
import gr.ml.analytics.domain.Rating
import io.swagger.annotations._

@Api(value = "Ratings", produces = "application/json", consumes = "application/json")
@Path("/ratings")
class RatingsAPI(val ratingService: RatingService) {

  val route: Route = postRatings

  @ApiOperation(httpMethod = "POST", value = "Create ratings")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "gr.ml.analytics.domain.gr.ml.analytics.domain.Rating definition", required = true, paramType = "body", collectionFormat = "List", dataType = "gr.ml.analytics.domain.gr.ml.analytics.domain.gr.ml.analytics.domain.Rating")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Ratings have been created"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  def postRatings: Route =
    path("ratings") {
      post {
        entity(as[List[Map[String, Any]]]) { ratings =>
          ratings.foreach { map =>
            ratingService.save(
              map("userId").toString.toInt,
              map("itemId").toString.toInt,
              map("rating").toString.toDouble,
              map.getOrElse("timestamp", (System.currentTimeMillis() / 1000).toInt).toString.toInt
            )
          }

          complete(StatusCodes.Created)
        }
      }
    }
}
