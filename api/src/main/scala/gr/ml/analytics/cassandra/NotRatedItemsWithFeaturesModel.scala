/*
package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.NotRatedItemWithFeatures

import scala.concurrent.Future

/**
  * Cassandra representation of the not_rated_items_with_features table
  */
abstract class NotRatedItemsWithFeaturesModel extends CassandraTable[ConcreteNotRatedItemsWithFeaturesModel, NotRatedItemWithFeatures] {

  override def tableName: String = "not_rated_items_with_features"

  object userId extends IntColumn(this) with PartitionKey

  object items extends MapColumn[Int, List[Double]](this)

  override def fromRow(r: Row): NotRatedItemWithFeatures = NotRatedItemWithFeatures(userId(r), items(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteNotRatedItemsWithFeaturesModel extends NotRatedItemsWithFeaturesModel with RootConnector {

  def getItemWithFeaturesNotRatedByUser(userId: Int): Future[List[Map[Int, java.util.List[Double]]]] = {
    select(_.items)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .fetch
  }

  def removeNotRatedItem(userId: Int, itemId: Int): Unit ={
    delete(_.items(itemId))
      .where(_.userId eqs userId)
      .future()
  }

  def addNotRatedItem(userId: Int, itemId: Int, features: List[Double]): Unit ={
    update
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .modify(_.items.put(itemId, features))
      .future()
  }

  def save(userId: Int, itemsWithFeatures: Map[Int, List[Double]]) = {
    update
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .modify(_.items.putAll(itemsWithFeatures))
      .future()
  }
}
*/
