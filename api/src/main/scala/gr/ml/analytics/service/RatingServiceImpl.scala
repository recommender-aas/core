package gr.ml.analytics.service

import java.util.Calendar

import com.datastax.driver.core.Row
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase

class RatingServiceImpl(inputDatabase: InputDatabase) extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val userModel = inputDatabase.userModel
  private lazy val ratingTimestampModel = inputDatabase.ratingTimestampModel
  private lazy val notRatedItemModel = inputDatabase.notRatedItemModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Int): Unit = {
    val keyspace = inputDatabase.ratingModel.keySpace
    val schemaId = 0; // TODO unhardcode!!!
    val itemsTableName = Util.itemsTableName(schemaId)
    val query1 = s"INSERT INTO $keyspace.ratings (userid, itemid, rating) values ($userId, $itemId, $rating)";
    val wasApplied1 = inputDatabase.connector.session.execute(query1).wasApplied()
    println("Saved into ratings table - " + wasApplied1)

    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(timestamp*1000L);
    val year = cal.get(Calendar.YEAR)
    val query2 = s"INSERT INTO $keyspace.ratings_timestamp (year, userid, timestamp) values ($year, $userId, $timestamp)";
    val wasApplied2 = inputDatabase.connector.session.execute(query2).wasApplied()
    println("Saved into ratings_timestamp table - " + wasApplied2)

    val query3 = s"SELECT movieid FROM $keyspace.$itemsTableName";
    val itemIds = inputDatabase.connector.session.execute(query3).all().toArray
      .map(r => r.asInstanceOf[Row].getInt(0)).toList;

    itemIds.foreach(itemId => {
      val query3 = s"INSERT INTO $keyspace.not_rated_items (userid, itemid) values ($userId, $itemId)";
      inputDatabase.connector.session.execute(query3)
    })
    val query4 = s"INSERT INTO $keyspace.users (userid) values ($userId)";
    val wasApplied4 = inputDatabase.connector.session.execute(query4).wasApplied()
    println("Saved user " + userId + " - " + wasApplied4);

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
