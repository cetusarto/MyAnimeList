package General_Analysis

import Helper._
import org.apache.spark.sql.functions._

object UserAnimeAnalysis extends App {
  val spark = Helper.getSparkSession("UserAnimeAnalysis")
  val user_animeDF = Helper.readParquetSchema(spark, "user_anime.parquet",SchemaHelper.getUserAnimeSchema)

  val fullCount = user_animeDF.count()
  val scoredCount = user_animeDF.where(col("score").isNotNull).count()


  // Reviewed and non scored percentage
  user_animeDF
    .withColumn("has_score", col("score").isNotNull)
    .select("has_score")
    .groupBy("has_score").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100) //.show()

  //Score status
  user_animeDF
    .where(col("score").isNotNull)
    .groupBy("status").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / scoredCount * 100)
  //.show()

  // Non Scored status
  user_animeDF
    .where(col("score").isNull)
    .groupBy("status").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / (fullCount - scoredCount) * 100)
    .show()

  user_animeDF
    .where(col("score").isNotNull or col("review_score").isNotNull)
    .selectExpr(
      "avg(score) as avg_score",
      "std(score) as std_score",
      "avg(review_score) as avg_review_score",
      "std(review_score) as std_review_score",
    ).show()

  user_animeDF
    .where(col("review_score").isNotNull)
    .selectExpr(
      "avg(score) as avg_score",
      "std(score) as std_score",
      "avg(review_score) as avg_review_score",
      "std(review_score) as std_review_score"
    ).show()

  user_animeDF
    .where(col("review_score").isNotNull)
    .selectExpr(
      "avg(score) as avg_score",
      "std(score) as std_score",
      "avg(review_score) as avg_review_score",
      "std(review_score) as std_review_score"
    ).show()


}
