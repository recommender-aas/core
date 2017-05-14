package gr.ml.analytics.cassandra

import com.datastax.driver.core.Row

import scala.concurrent.Future

class CassandraCache(inputDatabase: CassandraStorage) {

  private var allUserIDs: Set[Int] = null
  private var allItemIDs: Set[Int] = null
  private var schemas: Map[Int, Future[Option[Schema]]] = Map()


  def getAllUserIDs(): Set[Int] = {
    if (allUserIDs == null) {
      val keyspace = inputDatabase.ratingModel.keySpace
      val getAllUserIDsQuery = s"SELECT userid FROM $keyspace.users"
      allUserIDs = inputDatabase.connector.session.execute(getAllUserIDsQuery).all().toArray
        .map(r => r.asInstanceOf[Row].getInt(0)).toSet
    }
    allUserIDs
  }

  def getAllItemIDs(): Set[Int] = {
    if (allItemIDs == null) {
      val keyspace = inputDatabase.ratingModel.keySpace
      val getAllItemIDsQuery = s"SELECT itemid FROM $keyspace.items_0_dense"; // TODO unhardcode schema
      allItemIDs = inputDatabase.connector.session.execute(getAllItemIDsQuery).all().toArray
        .map(r => r.asInstanceOf[Row].getInt(0)).toSet
    }
    allItemIDs
  }

  def findSchema(id: Int): Future[Option[Schema]] = {
    val schemaOptional = schemas.get(id)
    schemaOptional match {
      case Some(schema) => schema
      case None =>
        val future = inputDatabase.schemasModel.getOne(id)
        schemas = schemas + (id -> future)
        future
    }

  }

  def invalidateUserIDs(): Unit = {
    allUserIDs = null
  }

  def invalidateItemIDs(): Unit = {
    allItemIDs = null
  }

  def invalidateSchemas(): Unit = {
    schemas = Map()
  }
}
