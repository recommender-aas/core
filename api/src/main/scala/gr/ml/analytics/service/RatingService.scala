package gr.ml.analytics.service

trait RatingService {

  /**
    * Create new ratings for a given user
    * @param userId id of the user who rated items
    */
  def save(userId: Int, itemId: Int, rating: Double, timestamp: Int)

}
