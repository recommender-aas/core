package gr.ml.analytics.service.popular

import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import gr.ml.analytics.service.{Source, SourceNew}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class PopularItemsJobNew(val source: SourceNew,
                      val config: Config)(implicit val sparkSession: SparkSession) {

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  def run(): Unit = {
    val ratingsDS = source.getAllRatings(ratingsTable)
      .select("userId", "itemId", "rating")
    val allRatings = ratingsDS.collect().map(r => List(r.getInt(0), r.getInt(1), r.getDouble(2)))

    val mostPopular: List[(Int, Double, Double, Int)] = allRatings.filter(l => l(1) != "itemId").groupBy(l => l(1))
      .map(t => (t._1, t._2, t._2.size))
      .map(t => (t._1, t._2.reduce((l1, l2) => List(l1(0), l1(1), (l1(2) + l2(2)))), t._3))
      .map(t => (0, t._1, t._2(2).toString.toDouble, t._3)) // take sum of ratings
      .toList.sortWith((tl, tr) => tl._2 > tr._2) // sorting by sum of ratings
      .take(allRatings.size / 10) // take first 1/10 of items sorted by sum of ratings

//    val maxRating: Double = mostPopular.sortWith((tl, tr) => tl._2.toDouble > tr._2.toDouble).head._2
//    val maxNumberOfRatings: Int = mostPopular.sortWith((tl, tr) => tl._3 > tr._3).head._3
//    val sorted: List[(Double, Double, Int)] = mostPopular.sortWith(sortByRatingAndPopularity(maxRating, maxNumberOfRatings))
//      .map(t => (t._1, t._2, t._3))
println("check") // TODO remove
    val popularItemsDF: DataFrame = mostPopular.toDF("dummy_partition", "itemid", "sum_ratings", "num_ratings")
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


object PopularItemsJobNew {

  def apply(source: SourceNew, config: Config)
           (implicit sparkSession: SparkSession): PopularItemsJobNew =
    new PopularItemsJobNew(source, config)
}