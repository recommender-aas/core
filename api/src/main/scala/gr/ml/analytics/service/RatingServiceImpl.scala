package gr.ml.analytics.service

import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.{Rating, RatingTimestamp}

class RatingServiceImpl(inputDatabase: InputDatabase) extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val ratingTimestampModel = inputDatabase.ratingTimestampModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Int): Unit = {
    val ratingEntity = Rating(userId, itemId, rating, timestamp)
    ratingModel.save(ratingEntity)
    val cal: Calendar = Calendar.getInstance();
    cal.setTimeInMillis(timestamp*1000L);
    val ratingTimestampEntity = RatingTimestamp(cal.get(Calendar.YEAR), userId, timestamp)
    ratingTimestampModel.save(ratingTimestampEntity)

    logger.info(s"saved $ratingEntity")
  }

}
