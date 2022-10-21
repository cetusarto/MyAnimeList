package General_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserAnimeAnalysis extends App {
  val spark = SparkSession.builder()
    .appName("User-Anime Analysis")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val user_animeDF = spark.read
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/data/user_anime.parquet"
    )).load()

  val fullCount = user_animeDF.count()
  val nonReviewCount = user_animeDF.where(col("review_id").isNull).count()

  // Reviewed and non reviewed percentage
  user_animeDF
    .withColumn("has_review", col("review_id").isNull)
    .select("has_review")
    .groupBy("has_review").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount).show()

  //Reviewed status
  user_animeDF
    .where(col("review_id").isNotNull)
    .groupBy("status").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / (fullCount - nonReviewCount) * 100).show()

  //Non reviewed status
  user_animeDF
    .where(col("review_id").isNull)
    .groupBy("status").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / nonReviewCount * 100).show()

}
