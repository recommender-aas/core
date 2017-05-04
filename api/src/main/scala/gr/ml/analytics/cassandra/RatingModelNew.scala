package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.{Rating, RatingNew}
import com.outworkers.phantom.dsl.ClusteringOrder

import scala.concurrent.Future

/**
  * Cassandra representation of the Ratings table
  */
class RatingModelNew extends CassandraTable[ConcreteRatingModelNew, RatingNew] {

  override def tableName: String = "ratings"

  object userId extends IntColumn(this) with PartitionKey

  object itemId extends IntColumn(this) with ClusteringOrder with Ascending

  object rating extends DoubleColumn(this)

  object features extends ListColumn[Double](this)

  override def fromRow(r: Row): RatingNew = RatingNew(userId(r), itemId(r), rating(r), features(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRatingModelNew extends RatingModelNew with RootConnector {

  def getAll: Future[List[RatingNew]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getRatingsForUser(userId: Int): Future[List[RatingNew]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .fetch
  }

  def save(rating: RatingNew) = {
    insert
      .value(_.userId, rating.userId)
      .value(_.itemId, rating.itemId)
      .value(_.rating, rating.rating)
      .value(_.features, rating.features)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}


