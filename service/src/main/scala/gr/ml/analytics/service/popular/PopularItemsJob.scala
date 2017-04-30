package gr.ml.analytics.service.popular

import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import gr.ml.analytics.service.Source
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

class PopularItemsJob(val source: Source,
                      val config: Config)(implicit val sparkSession: SparkSession) {

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  def run(): Unit = {
    val ratingsDS = source.getRatings(ratingsTable)
    // TODO how about having items_avg_rating table that we can update based on new-coming ratings?
    // (we need current avg_rating and a number of ratings for this item)
    val allRatings = ratingsDS.collect().map(r => List(r.getInt(0), r.getInt(1), r.getDouble(2)))

    // TODO Need to make the code cleaner
    // TODO we can use it as a general method both for files and cassandra
    val mostPopular = allRatings.filter(l => l(1) != "itemId").groupBy(l => l(1))
      .map(t => (t._1, t._2, t._2.size))
      .map(t => (t._1, t._2.reduce((l1, l2) => List(l1(0), l1(1), (l1(2) + l2(2)))), t._3))
      .map(t => (t._1, t._2(2).toString.toDouble / t._3.toDouble, t._3)) // calculate average rating
      .toList.sortWith((tl, tr) => tl._3 > tr._3) // sorting by number of ratings
      .take(allRatings.size / 10) // take first 1/10 of items sorted by number of ratings

    val maxRating: Double = mostPopular.sortWith((tl, tr) => tl._2.toInt > tr._2.toInt).head._2
    val maxNumberOfRatings: Int = mostPopular.sortWith((tl, tr) => tl._3 > tr._3).head._3

    val sorted = mostPopular.sortWith(sortByRatingAndPopularity(maxRating, maxNumberOfRatings))
      .map(t => (t._1, t._2, t._3))

    val popularItemsDF: DataFrame = sorted.toDF("itemid", "rating", "n_ratings")
    popularItemsDF
      .write.mode("overwrite")
      .cassandraFormat(popularItemsTable, keyspace)
      .save()
  }

  private def sortByRatingAndPopularity(maxRating: Double, maxRatingsNumber: Int) = {
    // Empirical coefficient to make popular high rated movies go first
    // (suppressing unpopular but high-rated movies by small number of individuals)
    // Math.PI is just for more "scientific" look ;-)
    val coef = Math.PI * Math.sqrt(maxRatingsNumber.toDouble) / Math.sqrt(maxRating)
    (tl: (Double, Double, Int), tr: (Double, Double, Int)) =>
      Math.sqrt(tl._3) + coef * Math.sqrt(tl._2) > Math.sqrt(tr._3) + coef * Math.sqrt(tr._2)
  }

}


object PopularItemsJob {

  def apply(source: Source, config: Config)
           (implicit sparkSession: SparkSession): PopularItemsJob =
    new PopularItemsJob(source, config)
}