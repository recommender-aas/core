package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, post, _}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.online.Interaction
import gr.ml.analytics.service.{ActionService, RatingService}
import io.swagger.annotations._
import spray.json.RootJsonFormat

import scala.annotation.meta.field
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Api(value = "Ratings", produces = "application/json", consumes = "application/json")
@Path("/ratings")
class InteractionsAPI(val ratingService: RatingService, val actionService: ActionService, onlineModelActorOption: Option[ActorRef]) {

  val route: Route = postInteraction

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "User interaction")
  case class InteractionView(
                              @(ApiModelProperty@field)(value = "Unique user identifier")
                              userId: Int,
                              @(ApiModelProperty@field)(value = "Unique item identifier")
                              itemId: Int,
                              @(ApiModelProperty@field)(value = "Optional name of user action. If not present the 'action' parameter is mandatory")
                              action: Option[String],
                              @(ApiModelProperty@field)(value = "Optional user rating. If not present the 'action' parameter is mandatory")
                              rating: Option[Double],
                              @(ApiModelProperty@field)(value = "Optional point of time when an interaction was performed. If not specified the timestamp is generated when this interaction is created", required = false)
                              timestamp: Option[Long])

  implicit val actionViewJsonFormat: RootJsonFormat[InteractionView] = jsonFormat5(InteractionView.apply)

  @ApiOperation(httpMethod = "POST", value = "Create interactions")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Interactions list", required = true, paramType = "body", collectionFormat = "List", dataType = "gr.ml.analytics.api.InteractionsAPI$InteractionView")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "All interactions created", responseContainer = "List"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Action not found for at least one interaction")))
  def postInteraction: Route =
    path("ratings") {
      post {
        entity(as[List[InteractionView]]) { interactions =>

          onSuccess(Future.sequence(
            interactions.map(interaction => {
              if (interaction.action.isDefined && interaction.rating.isDefined) {
                Future((interaction, None))
              } else if (interaction.action.isDefined) {
                actionService.getByName(interaction.action.get).map(actionOption => (interaction, actionOption.map(_.weight)))
              } else {
                Future((interaction, interaction.rating))
              }
            }))) {
            case list: List[(InteractionView, Option[Double])] =>

              // action or rating must exist for every interaction in a batch
              if (list.forall(_._2.isDefined)) {

                val processedInterractions = list.map(element => {
                  val weight: Double = element._2.get
                  val userId = element._1.userId
                  val itemId = element._1.itemId
                  val timestamp = element._1.timestamp.getOrElse(System.currentTimeMillis() / 1000).toString.toLong

                  val interaction: Interaction = Interaction(
                    userId.toString,
                    itemId.toString,
                    weight,
                    timestamp)

                  // batch learning
//                  ratingService.save(userId, itemId, weight, timestamp)

                  interaction
                })

                // online learning
                if (onlineModelActorOption.isDefined) {
                  onlineModelActorOption.get ! processedInterractions
                }

                complete(StatusCodes.Created)
              } else {
                complete(StatusCodes.BadRequest)
              }
            case _ =>
              complete(StatusCodes.InternalServerError)
          }
        }
      }
    }
}
