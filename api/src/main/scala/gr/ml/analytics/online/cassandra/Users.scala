package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.Future

case class User(
                 id: String,
                 items: Map[String, Double]
               )

class UsersTable extends CassandraTable[Users, User] {

  override def tableName: String = "users_table"

  object id extends StringColumn(this) with PartitionKey
  object items extends MapColumn[String, Double](this)
  override def fromRow(row: Row): User = {
    User(
      id(row),
      items(row)
    )
  }
}

abstract class Users extends UsersTable with RootConnector {

  def store(user: User): Future[ResultSet] = {
    insert.value(_.id, user.id).value(_.items, user.items)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def updateUser(user: User): Future[ResultSet] = {
    update.where(_.id eqs user.id).modify(_.items setTo user.items).future()
  }

  def getById(id: String): Future[Option[User]] = {
    select
      .where(_.id eqs id).one()
  }
}
