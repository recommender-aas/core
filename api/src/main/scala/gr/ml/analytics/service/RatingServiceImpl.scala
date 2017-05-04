package gr.ml.analytics.service

import java.util.Calendar

import com.datastax.driver.core.Row
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraCache, InputDatabase}

class RatingServiceImpl(val inputDatabase: InputDatabase, val cassandraCache: CassandraCache) extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val userModel = inputDatabase.userModel
  private lazy val ratingTimestampModel = inputDatabase.ratingTimestampModel
  private lazy val notRatedItemModel = inputDatabase.notRatedItemsModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Int): Unit = {
//    val ratingEntity = Rating(userId, itemId, rating, timestamp) // 20 secs
//    ratingModel.save(ratingEntity)
//    logger.info(s"saved $ratingEntity")

    val keyspace = inputDatabase.ratingModel.keySpace
    val schemaId = 0; // TODO unhardcode!!!
    val itemsTableName = Util.itemsTableName(schemaId)

    val insertRatingsQuery = s"INSERT INTO $keyspace.ratings (userid, itemid, rating) values ($userId, $itemId, $rating)";
    val start = System.currentTimeMillis()
    inputDatabase.connector.session.execute(insertRatingsQuery).wasApplied()

    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(timestamp*1000L);
    val year = cal.get(Calendar.YEAR)
    val insertRatingsTimestamp = s"INSERT INTO $keyspace.ratings_timestamp (year, userid, timestamp) values ($year, $userId, $timestamp)";
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




//  val ratingEntity = Rating(userId, itemId, rating, timestamp)
//  val saveRatingFuture = ratingModel.save(ratingEntity)
//  saveRatingFuture.onSuccess{
//    case rs : ResultSet =>
//      val cal: Calendar = Calendar.getInstance();
//      cal.setTimeInMillis(timestamp*1000L);
//      val ratingTimestampEntity = RatingTimestamp(cal.get(Calendar.YEAR), userId, timestamp)
//      val saveRatingTimestampFuture = ratingTimestampModel.save(ratingTimestampEntity)
//
//      saveRatingTimestampFuture.onSuccess {
//        case rs : ResultSet =>
//          val getUserFuture:Future[Option[User]] = userModel.getUserById(userId)
//          getUserFuture.onSuccess {
//            case result => result match {
//              case Some(value) =>
//                println("there is SOME user with id " + value.userId)
//              case None => {
//                //          println("no user with specified ID");
//                val schemaId = 0; // TODO unhardcode!!!
//                val tableName = Util.itemsTableName(schemaId)
//                val query = s"SELECT movieid FROM ${inputDatabase.ratingModel.keySpace}.$tableName";
//                val itemIds = inputDatabase.connector.session.execute(query).all().toArray
//                  .map(r => r.asInstanceOf[Row].getInt(0)).toList;
//                itemIds.foreach(itemId => notRatedItemModel.save(userId, itemId))
//                //          println("Received itemids = " + itemIds);
//                val saveUserFuture = userModel.save(User(userId))
//                saveUserFuture.onSuccess{
//                  case rs: ResultSet =>
//                    println("Finally saved user... ");
//                }
//              }
//            }
//
//          }
//      }
//
//
//
//  }
//
//
//
//
//
//  // TODO remove the row from 2 not rated tables
//
//  //    logger.info(s"saved $ratingEntity")
//}


}
