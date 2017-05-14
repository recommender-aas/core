package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.CassandraStorage
import gr.ml.analytics.online.ItemItemRecommender

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RecommenderServiceImpl(inputDatabase: CassandraStorage, itemItemRecommenderOption: Option[ItemItemRecommender]) extends RecommenderService with LazyLogging {

  private lazy val recommendationModel = inputDatabase.recommendationsModel

  /**
    * @inheritdoc
    */
  override def getTop(userId: Int, n: Int): Future[List[Int]] = {
    val recommendations = recommendationModel.getOne(userId).map {
      case Some(recommendation) => recommendation.topItems.take(n)
      case None => throw new RuntimeException()
    }

    if (itemItemRecommenderOption.isDefined) {
      // The logic is:
      // If model has been trained for a given user and predictions are stored return those predictions
      // If there is no model trained return result from online item-to-item CF algorithm
      recommendations recoverWith {
        case e: RuntimeException =>
          val res = itemItemRecommenderOption.get.getRecommendations(userId.toString, n)
            .map(seq => seq.map(pair => pair._1.toInt).toList)
          res
      }
    } else {
      recommendations
    }

  }
}
