package Data_Preparation

import Helper._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Uses Spark SQL functions to get rid of all interactions without review and saves it in a parquet file
 * */

object DataIntegrity extends App {

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

  def countCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      count(when(col(c).isNull, c)).alias(c)
    })
  }


  /**
   * Check reviews integrity (if there are nulls in any of the sub-reviews scores like enjoyement)
   */

  //  user_AnimeDF
  //    .where(col("review_id").isNotNull).agg(
  //    count("review_score"), count("review_story_score"), count("review_animation_score"), count("review_sound_score"),
  //    count("review_character_score"), count("review_enjoyment_score")).show()
  //  println("Check reviews integrity (if there are nulls in any of the sub-reviews scores like enjoyement)")
  //

  /**
   * Count Of Animes with no scores
   */
  //  val animeNoReviews = animeIdsDF.join(animeWScoresDF
  //    , animeIdsDF.col("id") === animeWScoresDF.col("anime_id"), "left_anti") //
  //  animeNoReviews.agg(count("*")).show()

  /**
   * Null count
   */
  //  userDF.select(countCols(userDF.columns): _*).show()
  //  animeDF.select(countCols(animeDF.columns): _*).show()
  //  user_AnimeDF.select(countCols(user_AnimeDF.columns): _*).show()
  //  println("Null count")

  /**
   * User_review date check
   */
  val date_userAnimeDF = user_AnimeDF.where(col("review_id").isNotNull).selectExpr("review_date as date")
    .withColumn("timestamp date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))

  date_userAnimeDF.show()
  println("Count of Unacceptable dates ", date_userAnimeDF.where(col("date").isNull or col("date") < lit(2014)).count())


}
