package General_Analysis

import Helper.Helper
import org.apache.spark.sql.functions._

object UserAnimeAnalysis extends App {
  val spark = Helper.getSparkSession("UserAnimeAnalysis")
  val user_animeDF = Helper.readParquet(spark, "user_anime.parquet")

  val fullCount = user_animeDF.count()
  val scoredCount = user_animeDF.where(col("score").isNotNull).count()


  // Reviewed and non scored percentage
  user_animeDF
    .withColumn("has_score", col("score").isNotNull)
    .select("has_score")
    .groupBy("has_score").agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100)//.show()

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


}
