package Next_Analysis

import Helper.Helper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Genres_UserAnime extends App {
  val spark = Helper.getSparkSession("Second question")


  val user_animeDF = Helper.readParquet(spark, "user_anime.parquet")
    .select("anime_id", "favorite", "review_score", "review_num_useful", "score", "review_id")
    .where(col("score").isNotNull or col("review_id").isNotNull)

  val animeGenresDF = Helper.readParquet(spark, "anime.parquet")
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
        count("review_id").as("reviews_given"),
        count("score").as("scores_given"),
        sum("favorite").as("favorites")
      )
      .orderBy(desc("genre"))

  genre_summaryDF.coalesce(1).write.format("csv")
    .option("header", "true")
    .mode(SaveMode.Overwrite).save("results/genre_summary.csv")
}
