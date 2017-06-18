package gr.ml.analytics

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.CorsDirectives._
import com.github.swagger.akka.SwaggerSite
import gr.ml.analytics.Configuration._
import gr.ml.analytics.api._
import gr.ml.analytics.api.swagger.SwaggerDocService
import gr.ml.analytics.cassandra.{CassandraConnector, CassandraStorage}
import gr.ml.analytics.online.cassandra.OnlineCassandraStorage
import gr.ml.analytics.online.{ItemItemRecommender, OnlineLearningActor}
import gr.ml.analytics.service._

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

  val database = new CassandraStorage(cassandraConnector.connector)
  val onlineDatabase = new OnlineCassandraStorage(cassandraConnector.connector)

  var onlineLearnerOption: Option[ItemItemRecommender] = None
  var onlineLearnerActorOption: Option[ActorRef] = None

  if (onlineLearningEnable) {
    // online recommender
    onlineLearnerOption = Some(new ItemItemRecommender(onlineDatabase))
    onlineLearnerActorOption = Some(
      system.actorOf(Props(new OnlineLearningActor(onlineLearnerOption.get)), "online_learning_actor"))
  }

  // create services
  val schemasService: SchemaService = new SchemaServiceImpl(database)
  val recommenderService: RecommenderService = new RecommenderServiceImpl(database, onlineLearnerOption)
  var itemsService: ItemService = new ItemServiceImpl(database)
  val ratingsService: RatingService = new RatingServiceImpl(database)
  val actionService: ActionService = new ActionServiceImpl(database)

  // create apis
  val recommenderApi = new RecommenderAPI(recommenderService)
  val itemsApi = new ItemsAPI(itemsService)
  val schemasApi = new SchemasAPI(schemasService)
  val actionsApi = new ActionsAPI(actionService)
  val ratingsApi = new InteractionsAPI(ratingsService, actionService, onlineLearnerActorOption)

  // enable cross origin requests
  // enable swagger
  val rotes = cors() (
    recommenderApi.route ~
      itemsApi.route ~
      schemasApi.route ~
      ratingsApi.route ~
      actionsApi.route ~
      new SwaggerDocService(s"$swaggerRestHost:$swaggerRestPort").routes ~
      new SwaggerSite {}.swaggerSiteRoute
  )

  val recommenderAPIBindingFuture = Http().bindAndHandle(
    rotes,
    interface = serviceListenerInterface,
    port = serviceListenerPort)


  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      List(recommenderAPIBindingFuture)
        .foreach(_.flatMap(_.unbind()) // trigger unbinding from the port
          .onComplete(_ ⇒ system.terminate())) // and shutdown when done
      log.info("Stopped")
    }
  })

}
