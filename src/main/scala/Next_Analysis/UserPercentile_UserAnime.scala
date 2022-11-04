package Next_Analysis

import Helper.Helper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object UserPercentile_UserAnime extends App {
  val spark = Helper.getSparkSession("First question")

  //val user_animeDF = Helper.readParquet(spark,"user_anime.parquet")
  val userDF = Helper.readParquet(spark, "user.parquet")


  /**
   * The following commented part gives each user their respective percentile given their watch time
   * and saves it in a file that is later used
   */

  //Getting 40 percentiles

//  val partitions = 40
//  val percentiles = for (i <- 0 to partitions toArray) yield i / partitions.toDouble
//  print(userDF1.stat.approxQuantile("rounded", percentiles, 0).distinct.mkString(","))
//
//  //User percentile classifies each user their respective percentile given their watch time
//  val userDF1 = userDF.withColumn("rounded", round(col("num_days")))
//  print(userDF1.groupBy("rounded").agg(count("*")).count())
//
//
//  val partitions = Array(0.0, 1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 22.0, 24.0, 27.0, 29.0, 31.0, 34.0, 36.0, 39.0, 41.0, 44.0, 47.0, 50.0, 53.0, 57.0, 61.0, 65.0, 69.0, 74.0, 79.0, 85.0, 92.0, 100.0, 109.0, 121.0, 137.0, 161.0, 206.0, 105339.0, 100000000.0)
//  val userPercentile = for (i <- 0 to 39) yield (partitions(i), partitions(i + 1), (i + 1) * 2.5)
//
//  import spark.implicits._
//
//  val perDF = userPercentile.toDF("lower", "upper", "percentile")
//  val joinCondition = perDF.col("lower") <= userDF1.col("rounded") and userDF1.col("rounded") < perDF.col("upper")
//  val userPercentileDF = userDF1.join(perDF, joinCondition)
//    .drop("lower", "upper")
//
//  //Writes User Percentile file
//  userPercentileDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/userPercentile.parquet")


  val userPercentileDF = Helper.readParquet(spark, "userPercentile.parquet").select("user_id", "percentile", "rounded")
  val user_animeDF = Helper.readParquet(spark, "user_anime.parquet")
    .select("user_id", "score", "review_score", "review_num_useful", "review_id")
    .where(col("score").isNotNull or col("review_id").isNotNull)


  val mainDF = user_animeDF.join(userPercentileDF, "user_id").drop("user_id")

  //Grouping by percentile
  val percentilesDF =
    mainDF
      .groupBy("percentile")
      .agg(
        count("review_id").as("reviews_given"),
        count("score").as("scores_given"),
        sum("review_num_useful").as("usefulReviews_given"),
        avg("score").as("average_score"),
        stddev("score").as("stddev_score"),
        avg("review_score").as("average_review_score"),
        stddev("review_score").as("stddev_review_score"))
      .orderBy(desc("percentile"))

  /*
    percentilesDF.coalesce(1).write.format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite).save("results/percentile.csv")
  */
}
