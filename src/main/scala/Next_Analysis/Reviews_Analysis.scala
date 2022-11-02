package Next_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Reviews_Analysis extends App {

  val spark = SparkSession.builder()
    .appName("Reviews Individual Analysis")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val user_anime_reviewsDF = spark.read
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/data/user_anime_review.parquet"
    )).load()

  val fullCount = user_anime_reviewsDF.count()
  val usefulSum = user_anime_reviewsDF.selectExpr("sum(review_num_useful)").head().getLong(0)

  // review_score distribution
  user_anime_reviewsDF
    .groupBy("review_score")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100)
    .orderBy(desc("review_score")).show()

  // Usefulness based on score
  user_anime_reviewsDF
    .groupBy("review_score")
    .agg(sum("review_num_useful").as("usefulness"))
    .withColumn("percentage", col("usefulness") / usefulSum * 100)
    .orderBy(desc("review_score")).show()

  //
  user_anime_reviewsDF
    .groupBy("review_score")


}
