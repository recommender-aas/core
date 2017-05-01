package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.NotRatedItem

import scala.concurrent.Future

/**
  * Cassandra representation of the not_rated_items table
  */
class NotRatedItemModel extends CassandraTable[ConcreteNotRatedItemModel, NotRatedItem] {

  override def tableName: String = "not_rated_items"

  object userId extends IntColumn(this) with PartitionKey

  object itemId extends IntColumn(this) with ClusteringOrder with Ascending

  override def fromRow(r: Row): NotRatedItem = NotRatedItem(userId(r), itemId(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteNotRatedItemModel extends NotRatedItemModel with RootConnector {

  def getItemIdsNotRatedByUser(userId: Int): Future[List[Int]] = {
    select(_.itemId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .fetch
  }

// TODO we would need a remove method (here or there)

  // TODO we can parallel it to make faster
  def save(userId: Int, itemId: Int) = {
    insert
      .value(_.userId, userId)
      .value(_.itemId, itemId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}