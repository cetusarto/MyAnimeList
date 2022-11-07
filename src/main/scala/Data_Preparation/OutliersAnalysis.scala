package Data_Preparation

import Helper._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column}


object OutliersAnalysis extends App {

  val spark = Helper.getSparkSession()

  //Main Tables to DFs
  val animeDF = Helper.readParquetSchema(spark, "anime.parquet", SchemaHelper.getAnimeSchema)
  val userDF = Helper.readParquetSchema(spark, "user.parquet", SchemaHelper.getUserSchema)
  val user_AnimeDF = Helper.readParquetSchema(spark, "user_anime.parquet", SchemaHelper.getUserAnimeSchema)

  //Sub DFs
  val animeWScoresDF = user_AnimeDF
    .where(col("score").isNotNull).select("anime_id")
    .groupBy("anime_id").agg(count("*").as("count")) // count of scores by anime_id
  val animeIdsDF = animeDF
    .selectExpr("anime_id as id").distinct() // all anime ids
  val genresDF = animeDF
    .select("anime_id", "genres")
    .withColumn("genre", explode(split(col("genres"), "\\|")))
    .drop("genres") //genres with id
  val userWScoresDF = user_AnimeDF
    .where(col("score").isNotNull).select("user_id")
    .groupBy("user_id").agg(count("*").as("count"))

  //Functions
  val detectOutlier = (values: Column, UpperLimit: Column, LowerLimit: Column) => {
    (values < LowerLimit) or (values > UpperLimit)
  }


  /**
   * Anime reviews outliers
   */

  val user_animeScoresDF = animeWScoresDF
    .withColumn("mean", avg("count").over())
    .withColumn("stddev", stddev("count").over())
    .withColumn("UpperLimit", col("mean") + col("stddev") * 3)
    .withColumn("LowerLimit", col("mean") - col("stddev") * 3).drop("mean", "stddev")
    .withColumn("outlier", detectOutlier(col("count"), col("UpperLimit"), col("LowerLimit")))

  user_animeScoresDF.groupBy("outlier").agg(count("*").as("count")).show()
  println("Anime reviews outliers")


  /**
   * Genres Outliers
   */

  val genresCount = genresDF.join(animeWScoresDF, "anime_id").groupBy("genre").agg(sum("count").as("sum_count"))
  val genresReviewsDF = genresCount
    .withColumn("mean", avg("sum_count").over())
    .withColumn("stddev", stddev("sum_count").over())
    .withColumn("UpperLimit", col("mean") + col("stddev") * 3)
    .withColumn("LowerLimit", col("mean") - col("stddev") * 3).drop("mean", "stddev")
    .withColumn("outlier", detectOutlier(col("sum_count"), col("UpperLimit"), col("LowerLimit")))

  genresReviewsDF.groupBy("outlier").agg(count("*")).show()

  /**
   * Users Outliers
   */
  //Reviews made by user outliers
  val userReviewDF = userWScoresDF
    .withColumn("mean", avg("count").over())
    .withColumn("stddev", stddev("count").over())
    .withColumn("UpperLimit", col("mean") + col("stddev") * 6)
    .withColumn("LowerLimit", col("mean") - col("stddev") * 6).drop("mean", "stddev")
    .withColumn("outlier", detectOutlier(col("count"), col("UpperLimit"), col("LowerLimit")))

  userReviewDF.groupBy("outlier").agg(count("*")).show()
  println("Users Outliers")

  //Watchtime Outliers
  val userWatchTime = userDF.select("user_id", "num_days")
    .withColumn("mean", avg("num_days").over())
    .withColumn("stddev", stddev("num_days").over())
    .withColumn("UpperLimit", col("mean") + col("stddev") * 3)
    .withColumn("LowerLimit", col("mean") - col("stddev") * 3).drop("mean", "stddev")
    .withColumn("outlier", detectOutlier(col("num_days"), col("UpperLimit"), col("LowerLimit")))

  userWatchTime.groupBy("outlier").agg(count("*")).show()
  println("Users Watch Time Outliers")


}
