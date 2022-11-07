package Next_Analysis

import Helper._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Genres_UserAnime extends App {
  val spark = Helper.getSparkSession("Second question")


  val user_animeDF = Helper.readParquetSchema(spark, "user_anime.parquet", SchemaHelper.getUserAnimeSchema)
    .select("anime_id", "favorite", "review_score", "score", "review_id", "review_num_useful")
    .where(col("score").isNotNull or col("review_id").isNotNull)
    .groupBy("anime_id")
    .agg(sum("favorite").as("favorite"), sum("score").as("score"),
      sum("review_score").as("review_score"), sum("review_num_useful").as("review_num_useful"))

  val animeGenresDF = Helper.readParquetSchema(spark, "anime.parquet", SchemaHelper.getAnimeSchema)
    .select("anime_id", "genres")
    .withColumn("genre", explode(split(col("genres"), "\\|"))).drop("genres")

  val genre_animeDF = user_animeDF.join(animeGenresDF, "anime_id")

  val genre_summaryDF =
    genre_animeDF
      .groupBy("genre")
      .agg(
        avg("score").as("average_score"),
        stddev("score").as("stddev_score"),
        avg("review_score").as("average_review_score"),
        stddev("review_score").as("stddev_review_score"),
        count("review_score").as("reviews_given"),
        count("score").as("scores_given"),
        sum("favorite").as("favorites")
      )
      .orderBy(desc("genre"))

  genre_summaryDF.coalesce(1).write.format("csv")
    .option("header", "true")
    .mode(SaveMode.Overwrite).save("results/genre_summary.csv")
}
