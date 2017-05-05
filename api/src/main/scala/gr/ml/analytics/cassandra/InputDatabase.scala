package gr.ml.analytics.cassandra

import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl.KeySpaceDef
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * The database which stores user input such as ratings and items.
  * Currently only ratings are supported.
  *
  * @param connector Cassandra connector
  */
class InputDatabase(override val connector: KeySpaceDef) extends Database[InputDatabase](connector) with LazyLogging {

  object recommendationsModel extends ConcreteRecommendationModel with connector.Connector

  object ratingModel extends ConcreteRatingModelNew with connector.Connector

  object userModel extends ConcreteUserModel with connector.Connector

  object ratingTimestampModel extends ConcreteRatingTimestampModel with connector.Connector

  object schemasModel extends ConcreteSchemaModel with connector.Connector

  object clusteredItemsModel extends ConcreteClusteredItemsModel with connector.Connector

  object notRatedItemsModel extends ConcreteNotRatedItemsModel with connector.Connector

//  object notRatedItemsWithFeaturesModel extends ConcreteNotRatedItemsWithFeaturesModel with connector.Connector

  // create tables if not exist
  private val f1 = schemasModel.create.ifNotExists().future()
  private val f2 = recommendationsModel.create.ifNotExists().future()
  private val f3 = ratingModel.create.ifNotExists().future()
  private val f4 = clusteredItemsModel.create.ifNotExists().future()
  private val f5 = ratingTimestampModel.create.ifNotExists().future()
  private val f6 = userModel.create.ifNotExists().future()
  private val f7 = notRatedItemsModel.create.ifNotExists().future()
//  private val f8 = notRatedItemsWithFeaturesModel.create.ifNotExists().future()

  try {
    Await.ready(f1, 3.seconds)
    Await.ready(f2, 3.seconds)
    Await.ready(f3, 3.seconds)
    Await.ready(f4, 3.seconds)
    Await.ready(f5, 3.seconds)
    Await.ready(f6, 3.seconds)
    Await.ready(f7, 3.seconds)
//    Await.ready(f8, 3.seconds)
    // TODO temporary workaround, I hope (until we cannot use Phantom table model)
    connector.session.execute("create table if not exists rs_new_keyspace.not_rated_items_with_features (userid int primary key, items map<int, frozen<list<double>>>)")
  } catch {
    case e: Throwable =>
      //ignore
      logger.warn("Error creating models", e)
  }

}
