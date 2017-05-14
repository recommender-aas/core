package gr.ml.analytics.cassandra

import com.datastax.driver.core.Row

class CassandraCache(inputDatabase: CassandraStorage) {

  private var allUserIDs: Set[Int] = null
  private var allItemIDs: Set[Int] = null


  def getAllUserIDs(): Set[Int] ={
    if (allUserIDs == null) {
      val keyspace = inputDatabase.ratingModel.keySpace
      val getAllUserIDsQuery = s"SELECT userid FROM $keyspace.users";
      allUserIDs = inputDatabase.connector.session.execute(getAllUserIDsQuery).all().toArray
        .map(r => r.asInstanceOf[Row].getInt(0)).toSet;
    }
    allUserIDs
  }

  def getAllItemIDs(): Set[Int] = {
    if (allItemIDs == null) {
      val keyspace = inputDatabase.ratingModel.keySpace
      val getAllItemIDsQuery = s"SELECT itemid FROM $keyspace.items_0_dense"; // TODO unhardcode schema
      allItemIDs = inputDatabase.connector.session.execute(getAllItemIDsQuery).all().toArray
        .map(r => r.asInstanceOf[Row].getInt(0)).toSet;
    }
    allItemIDs
  }

  def invalidateUserIDs(): Unit ={
    allUserIDs = null
  }

  def invalidateItemIDs(): Unit ={
    allItemIDs = null
  }
}
