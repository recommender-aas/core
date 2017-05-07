package gr.ml.analytics.cassandra


import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.Recommendation

import scala.concurrent.Future

/**
  * Cassandra representation of the Ratings table
  */
class RecommendationModel extends CassandraTable[ConcreteRecommendationModel, Recommendation] {

  override def tableName: String = "final_recommendations"

  object userId extends IntColumn(this) with PartitionKey

  object itemIds extends ListColumn[Int](this)

  override def fromRow(r: Row): Recommendation = Recommendation(userId(r), itemIds(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRecommendationModel extends RecommendationModel with RootConnector {

  def getAll: Future[List[Recommendation]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getOne(userId: Int): Future[Option[Recommendation]] = {
    select
      .where(_.userId eqs userId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .one
  }

  def save(userId: Int, itemIDs: List[Int]) = {
    insert
      .value(_.userId, userId)
      .value(_.itemIds, itemIDs)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}
