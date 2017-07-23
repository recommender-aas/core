package gr.ml.analytics.batch.cf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}


object CFJobRunner {

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark Recommendation Service")
      .getOrCreate
  }

  import java.io.IOException

  private def getFileWithUtil(fileName: String): Option[String] = {
    try
      Some(getClass.getClassLoader.getResource(fileName).toString)
    catch {
      case e: IOException =>
        e.printStackTrace()
        None
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val sparkSession = getSparkSession

    val source = new Source {
      override def getUserIds = Set(1, 7, 8, 10)
      override def getTrainingRatings: DataFrame = sparkSession.read
        .option("header", "true")
        .csv(getFileWithUtil("ratings.csv").get)
        .select(
          col("userid").cast(IntegerType),
          col("itemid").cast(IntegerType),
          col("rating").cast(DoubleType),
          col("timestamp").cast(LongType))
    }

    val sink = new Sink {
      var predictions = Map(
        1 -> List(11, 9, 10, 12, 14, 15, 13),
        7 -> List(1, 4, 6, 5, 15, 3, 2),
        8 -> List(7, 1, 6, 4, 5, 3, 2),
        10 -> List(6, 10, 9, 5, 3, 2)
      )

      override def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int], latestTimestamp: Long): Unit = {
        val expectedPredictions = predictions(userId)
        if (!expectedPredictions.equals(recommendedItemIds)) throw new Exception(s"user: $userId. expected: $expectedPredictions. actual $recommendedItemIds")
        println(s"UserId: $userId")
      }
    }

    val params = Map(
      "cf_rank"-> 5,
      "cf_max_iter" -> 2,
      "cf_reg_param" -> 1.0
    )

    val cfJob = new CFJob(source, sink, params)

    cfJob.run()

  }
}
