package gr.ml.analytics

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import Configuration._
import gr.ml.analytics.api.{ItemsAPI, RatingsAPI, RecommenderAPI, SchemasAPI}
import gr.ml.analytics.cassandra.{CassandraCache, CassandraConnector, InputDatabase}
import gr.ml.analytics.client.SchemasAPIClient
import gr.ml.analytics.service._
import akka.http.scaladsl.server.Directives._
import ch.megard.akka.http.cors.CorsDirectives._
import com.github.swagger.akka.SwaggerSite
import gr.ml.analytics.api.swagger.{SwaggerDocService, SwaggerUI}

import scala.io.StdIn


/**
  * Application entry point
  */
object Application extends App {

  implicit val system = ActorSystem("recommendation-service")

  /**
    * Ensure that the constructed ActorSystem is shut down when the JVM shuts down
    */
  sys.addShutdownHook(system.terminate())

  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  val log = Logging(system, getClass)


  def cassandraConnector = CassandraConnector(
    cassandraHosts,
    cassandraKeyspace,
    Some(cassandraUsername),
    Some(cassandraPassword))

  val inputDatabase = new InputDatabase(cassandraConnector.connector)
  val cassandraCache = new CassandraCache(inputDatabase)
  // create services
  val schemasService: SchemaService = new SchemaServiceImpl(inputDatabase)
  val schemasClient: SchemasAPIClient = new SchemasAPIClient(serviceClientURI)

  val recommenderService: RecommenderService = new RecommenderServiceImpl(inputDatabase)
  var itemService: ItemService = new ItemServiceImpl(inputDatabase, cassandraCache)
  val ratingsService: RatingService = new RatingServiceImpl(inputDatabase, cassandraCache, itemService)

  // create apis
  val recommenderApi = new RecommenderAPI(recommenderService)
  val itemsApi = new ItemsAPI(itemService)
  val schemasApi = new SchemasAPI(schemasService)
  val ratingsApi = new RatingsAPI(ratingsService)

  // enable cross origin requests
  // enable swagger
  val rotes = cors() (
    recommenderApi.route ~
      itemsApi.route ~
      schemasApi.route ~
      ratingsApi.route ~
      new SwaggerDocService(serviceListenerInterface, serviceListenerPort).routes ~
      new SwaggerSite {}.swaggerSiteRoute)

  val recommenderAPIBindingFuture = Http().bindAndHandle(
    rotes,
    interface = serviceListenerInterface,
    port = serviceListenerPort)

  StdIn.readLine() // let it run until user presses return

  List(recommenderAPIBindingFuture)
    .foreach(_.flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ system.terminate())) // and shutdown when done

  log.info("Stopped")
}
