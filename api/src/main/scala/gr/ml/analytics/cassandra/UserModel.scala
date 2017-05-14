package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl.{ClusteringOrder, _}
import gr.ml.analytics.domain.User

import scala.concurrent.Future

/**
  * Cassandra representation of the Users table
  */
class UserModel extends CassandraTable[ConcreteUserModel, User] {

  override def tableName: String = "users"

  object userId extends IntColumn(this) with PartitionKey

  override def fromRow(r: Row): User = User(userId(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteUserModel extends UserModel with RootConnector {

  def getAllUserIds(): Future[List[Int]] = {
    select(_.userId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getUserById(userId: Int): Future[Option[User]] = {
    select
    .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.userId eqs userId)
      .one
  }

  def save(user: User) = {
    insert
      .value(_.userId, user.userId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}