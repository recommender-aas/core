package gr.ml.analytics.cassandra

import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl.KeySpaceDef
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CassandraStorage(override val connector: KeySpaceDef) extends Database[CassandraStorage](connector) with LazyLogging {

  object recommendationsModel extends ConcreteRecommendationModel with connector.Connector
  object ratingModel extends ConcreteRatingModelNew with connector.Connector
  object userModel extends ConcreteUserModel with connector.Connector
  object ratingTimestampModel extends ConcreteRatingTimestampModel with connector.Connector
  object schemasModel extends ConcreteSchemaModel with connector.Connector
  object clusteredItemsModel extends ConcreteClusteredItemsModel with connector.Connector
  object actionsModel extends Actions with connector.Connector

//  object notRatedItemsWithFeaturesModel extends ConcreteNotRatedItemsWithFeaturesModel with connector.Connector

  try {
    Await.ready(createAsync(), 20.seconds)
    // TODO temporary workaround, I hope (until we cannot use Phantom table model)
    connector.session.execute("create table if not exists rs_keyspace.not_rated_items_with_features (userid int primary key, items map<int, frozen<list<double>>>)")
  } catch {
    case e: Throwable =>
      //ignore
      logger.warn("Error creating models", e)
  }


}
