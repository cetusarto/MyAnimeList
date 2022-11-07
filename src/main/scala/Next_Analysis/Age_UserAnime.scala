package Next_Analysis

import Helper._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object Age_UserAnime extends App {
  val spark = Helper.getSparkSession("Third question")


  val user_animeDF = Helper.readParquetSchema(spark, "user_anime.parquet",SchemaHelper.getUserAnimeSchema)
    .select("anime_id", "favorite", "review_score", "score", "review_id", "review_num_useful")
    .where(col("score").isNotNull or col("review_id").isNotNull)
    .groupBy("anime_id")
    .agg(sum("favorite").as("favorite"),sum("score").as("score"),
      sum("review_score").as("review_score"),sum("review_num_useful").as("review_num_useful"))

  val animeAgeDF = Helper.readParquetSchema(spark, "anime.parquet",SchemaHelper.getAnimeSchema)
    .select("anime_id", "start_date","end_date")
    .withColumn("start_year", substring(col("start_date"), 1, 4))
    .withColumn("end_year", substring(col("end_date"), 1, 4))
    .drop("start_date")
    .drop("end_date")

  val age_animeDF = user_animeDF.join(animeAgeDF, "anime_id")

  val age_sumDF =
    age_animeDF
      .groupBy("start_year")
      .agg(
        avg("score").as("average_score"),
        stddev("score").as("stddev_score"),
        avg("review_score").as("average_review_score"),
        stddev("review_score").as("stddev_review_score"),
        count("review_score").as("reviews_given"),
        count("score").as("scores_given"),
        sum("review_num_useful").as("usefulReviews_given")
      )
      .orderBy(desc("start_year"))


  //Printing results

  age_sumDF.coalesce(1).write.format("csv")
    .option("header", "true")
    .mode(SaveMode.Overwrite).save("results/age.csv")


}
