package gr.ml.analytics.cassandra

import java.util.Calendar

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl.{ClusteringOrder, _}
import gr.ml.analytics.domain.{Rating, RatingTimestamp}

import scala.concurrent.Future

/**
  * Cassandra representation of the Ratings table
  */
class RatingTimestampModel extends CassandraTable[ConcreteRatingTimestampModel, RatingTimestamp] {

  override def tableName: String = "ratings_timestamp"

  object year extends IntColumn(this) with PartitionKey

  object timestamp extends IntColumn(this) with ClusteringOrder with Descending

  object userId extends IntColumn(this) with ClusteringOrder with Ascending

  override def fromRow(r: Row): RatingTimestamp = RatingTimestamp(year(r), timestamp(r), userId(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRatingTimestampModel extends RatingTimestampModel with RootConnector {

  def getUserIdsForLastNSeconds(lastNSecs: Int): Future[List[Int]] = {
    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(System.currentTimeMillis());

    select(_.userId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.year eqs cal.get(Calendar.YEAR))
      .and(_.timestamp gte (System.currentTimeMillis()/1000).toInt - lastNSecs)
      .fetch
  }

  def save(ratingTimestamp: RatingTimestamp) = {
    insert
      .value(_.year, ratingTimestamp.year)
      .value(_.userId, ratingTimestamp.userId)
      .value(_.timestamp, ratingTimestamp.timestamp)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}