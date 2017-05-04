package gr.ml.analytics.service

import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraCache, InputDatabase}
import scala.concurrent.ExecutionContext.Implicits.global

class RatingServiceImpl(val inputDatabase: InputDatabase, val cassandraCache: CassandraCache, val itemService: ItemService)
  extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val userModel = inputDatabase.userModel
  private lazy val ratingTimestampModel = inputDatabase.ratingTimestampModel
  private lazy val notRatedItemModel = inputDatabase.notRatedItemsModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Int): Unit = {
    val keyspace = inputDatabase.ratingModel.keySpace

    val start = System.currentTimeMillis()
    val getItemQuery = s"SELECT features from $keyspace.items_0_dense where itemid = $itemId"; // TODO unhardcode!
    val featuresString = inputDatabase.connector.session.execute(getItemQuery).one().getObject("features").toString

    val insertRatingsQuery = s"INSERT INTO $keyspace.ratings (userid, itemid, rating, features) values ($userId, $itemId, $rating, $featuresString)"; // TODO try with phantom to check if this is quicker
    inputDatabase.connector.session.execute(insertRatingsQuery).wasApplied()

    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(timestamp*1000L);
    val year = cal.get(Calendar.YEAR)
    val insertRatingsTimestamp = s"INSERT INTO $keyspace.ratings_timestamp (year, userid, timestamp) values ($year, $userId, $timestamp)"; // TODO try with phantom to check if this is quicker
    inputDatabase.connector.session.execute(insertRatingsTimestamp).wasApplied()

    val getUserQuery = s"SELECT * FROM $keyspace.users WHERE userid = $userId";
    val foundUsers = inputDatabase.connector.session.execute(getUserQuery).all()

    if(foundUsers.size == 0){
      val itemIds = cassandraCache.getAllItemIDs()
      notRatedItemModel.save(userId, itemIds)

      val insertIntoUsersQuery = s"INSERT INTO $keyspace.users (userid) values ($userId)";
      inputDatabase.connector.session.execute(insertIntoUsersQuery)
      val finish = System.currentTimeMillis()
      println("Saved user " + userId + " for " + (finish - start) + " millis.");
      cassandraCache.invalidateUserIDs()
    }

    notRatedItemModel.removeNotRatedItem(userId, itemId)

    // TODO remove the row from 2 not rated tables

  }
}
