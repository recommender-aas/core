package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.NotRatedItem

import scala.concurrent.Future

/**
  * Cassandra representation of the items_not_rated_by_user table
  */
abstract class NotRatedItemModel extends CassandraTable[ConcreteNotRatedItemsModel, NotRatedItem] {

  override def tableName: String = "items_not_rated_by_user"

  object userId extends IntColumn(this) with PartitionKey

  object itemIds extends SetColumn[Int](this)

  override def fromRow(r: Row): NotRatedItem = NotRatedItem(userId(r), itemIds(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteNotRatedItemsModel extends NotRatedItemModel with RootConnector {

  def getItemIdsNotRatedByUser(userId: Int): Future[List[Set[Int]]] = {
    select(_.itemIds)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .fetch
  }

  def removeNotRatedItem(userId: Int, itemId: Int): Unit ={
    update
      .where(_.userId eqs userId)
      .modify(_.itemIds remove itemId)
      .future()
  }

  def addNotRatedItem(userId: Int, itemId: Int): Unit ={
    update
      .where(_.userId eqs userId)
      .modify(_.itemIds add itemId)
      .future()
  }

  def save(userId: Int, itemIds: Set[Int]) = {
    insert
      .value(_.userId, userId)
      .value(_.itemIds, itemIds)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}