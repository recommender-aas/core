package gr.ml.analytics.api

import java.util.UUID
import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.Schema
import gr.ml.analytics.service.{SchemaService, Util}
import io.swagger.annotations._
import spray.json.{JsArray, JsObject, JsString, JsValue, JsonFormat}

import scala.util.{Failure, Success, Try}


@Api(value = "Schemas", produces = "application/json", consumes = "application/json")
@Path("/schemas")
class SchemasAPI(schemaService: SchemaService) extends LazyLogging {

  val route: Route = postSchema ~ getSchema ~ getSchemas

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  implicit val anyFormat: JsonFormat[Any] = AnyJsonFormat
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case u: UUID => JsString(u.toString)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
    }
    def read(value: JsValue) = value match {
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
    }
  }

  @ApiOperation(httpMethod = "POST", value = "Create schema")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Schema definition", required = true, paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Schema has been created"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 500, message = "Internal server error")))
  def postSchema: Route =
    path("schemas") {
      post {
        entity(as[Map[String, Any]]) { jsonSchema =>
          val schemaId = UUID.randomUUID()
          val schema = Schema(schemaId, jsonSchema)

          Try {
            val normalizedStringSchema = Util.schemaToString(jsonSchema)

            logger.info(s"Saving $normalizedStringSchema")

            schemaService.save(schema)
          } match {
            case Success(schemaId) => complete(StatusCodes.Created, schemaView(schema))
            case Failure(ex) => complete(StatusCodes.BadRequest, s"Not created: ${ex.getMessage}")
          }

        }
      }
    }

  @ApiOperation(httpMethod = "GET", response = classOf[Map[String, Any]], value = "Get all schemas")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request")
  ))
  def getSchemas: Route =
    path("schemas") {
      get {
        onSuccess(schemaService.getAll) { res =>
          complete(StatusCodes.OK, res.map(schemaView))
        }
      }
    }

  @ApiOperation(httpMethod = "GET", response = classOf[Map[String, Any]], value = "Get schema by id")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "string", paramType = "path", value = "schema id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  @Path("/{schemaId}")
  def getSchema: Route =
    path("schemas" / JavaUUID) { schemaId =>
      get {
        onSuccess(schemaService.get(schemaId)) {

          case Some(schema: Schema) =>
            complete(StatusCodes.OK, schemaView(schema))
          case None => complete(StatusCodes.NotFound)
        }
      }
    }

  private def schemaView(schema: Schema): Map[String, Any] =
    Map[String, Any](
      "id" -> schema.schemaId,
      "schema" -> schema.jsonSchema
    )
}
