package gr.ml.analytics.api

import java.util.UUID
import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import spray.json.{JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat}

import scala.concurrent.Future
import gr.ml.analytics.service.ItemService
import io.swagger.annotations._

import scala.concurrent.ExecutionContext.Implicits.global


@Api(value = "Items", produces = "application/json", consumes = "application/json")
@Path("/schemas/{schemaId}/items")
class ItemsAPI(val itemService: ItemService) {

  val route: Route = postItems ~ getItem

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "An item object")
  type ItemView = Map[String, Any]

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case d: Double => JsNumber(d)
      case f: Float => JsNumber(f)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
      case x => throw new RuntimeException
    }

    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => throw new RuntimeException
    }
  }

  @ApiOperation(httpMethod = "POST", value = "Add new items")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "integer", paramType = "path", value = "Schema identifier of an item"),
    new ApiImplicitParam(name = "body", value = "Items definition", required = true, paramType = "body", dataType = "java.util.Map")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Items have been created", response = classOf[Int], responseContainer = "List"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  def postItems: Route =
    path("schemas" / JavaUUID / "items") { schemaId =>
      post {
        entity(as[List[ItemView]]) { items =>

          onSuccess(Future.sequence(items.map { item => itemService.save(schemaId, item) })) { idList: List[Option[Int]] =>
            complete(StatusCodes.Created, idList.filter(_.isDefined).map(_.get))
          }

        }
      }
    }

  @ApiOperation(httpMethod = "GET", response = classOf[ItemView], value = "Get item by id")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "integer", paramType = "path", value = "Schema identifier of an item"),
    new ApiImplicitParam(name = "itemId", required = true, dataType = "integer", paramType = "path", value = "Item identifier")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Item or schema not found")))
  @Path("/{itemId}")
  def getItem: Route =
    path("schemas" / JavaUUID / "items" / IntNumber) { (schemaId: UUID, itemId: Int) =>
      get {
        onSuccess(itemService.get(schemaId, itemId)) {
          case Some(item: ItemView) => complete(item)
          case None => complete(StatusCodes.NotFound, "Schema or item has not been found")
        }
      }
    }
}
