package gr.ml.analytics.service

import java.util.Calendar

import com.datastax.driver.core.Row
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraCache, CassandraStorage}
import gr.ml.analytics.domain.{RatingNew, RatingTimestamp, User}
import gr.ml.analytics.cassandra.{CassandraStorage, Rating}

class RatingServiceImpl(val inputDatabase: CassandraStorage, val cassandraCache: CassandraCache, val itemService: ItemService)
  extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val userModel = inputDatabase.userModel
  private lazy val ratingTimestampModel = inputDatabase.ratingTimestampModel

//  private lazy val notRatedItemsWithFeaturesModel = inputDatabase.notRatedItemsWithFeaturesModel

  def arrayListToList(arrayList: Object): List[Double] ={
    arrayList.asInstanceOf[java.util.ArrayList[Double]].toArray.toList.asInstanceOf[List[Double]]
  }
  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Int): Unit = {
    val keyspace = inputDatabase.ratingModel.keySpace

    val start = System.currentTimeMillis()
    val getItemQuery = s"SELECT features from $keyspace.items_0_dense where itemid = $itemId"; // TODO unhardcode!
    val features = inputDatabase.connector.session.execute(getItemQuery).one().getObject("features")
    val ratingObject = new RatingNew(userId, itemId, rating, arrayListToList(features))
    ratingModel.save(ratingObject)

    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(timestamp*1000L);
    ratingTimestampModel.save(new RatingTimestamp(cal.get(Calendar.YEAR), userId, timestamp))

    val getUserQuery = s"SELECT * FROM $keyspace.users WHERE userid = $userId";
    val foundUsers = inputDatabase.connector.session.execute(getUserQuery).all()

    if(foundUsers.size == 0){
      val getAllItemsAndFeaturesQuery = s"SELECT itemid, features FROM $keyspace.items_0_dense"; // TODO unhardcode schema
      val allItemsWithFeaturesMap = inputDatabase.connector.session.execute(getAllItemsAndFeaturesQuery).all().toArray
        .map(r => (r.asInstanceOf[Row].getInt(0), arrayListToList(r.asInstanceOf[Row].getObject(1)))).toMap
//      notRatedItemsWithFeaturesModel.save(userId, allItemsWithFeaturesMap)

      val jsonMapEntries = allItemsWithFeaturesMap.map(t => t._1 + " : " + "[" + t._2.toArray.mkString(", ") + "]")
      val jsonMap = "{" + jsonMapEntries.mkString(", ") + "}"
      val addNotRatedItemsWithFeaturesQuery = s"UPDATE $keyspace.not_rated_items_with_features set items = items + $jsonMap where userid = $userId"
      inputDatabase.connector.session.execute(addNotRatedItemsWithFeaturesQuery)

      userModel.save(new User(userId))
      val finish = System.currentTimeMillis()
      println("Saved user " + userId + " for " + (finish - start) + " millis.");
      cassandraCache.invalidateUserIDs()
    }

    val removeNotRatedItemWithFeaturesQuery = s"DELETE items[$itemId] from $keyspace.not_rated_items_with_features where userid = $userId"
    inputDatabase.connector.session.execute(removeNotRatedItemWithFeaturesQuery)
//    notRatedItemsWithFeaturesModel.removeNotRatedItem(userId, itemId)
  }
}
