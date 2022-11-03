package Data_Preparation

import Helper.Helper
import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.functions.col

/**
 * Uses Spark SQL functions to get rid of all interactions without review and saves it in a parquet file
 * */

object CleansingDataset extends App {


  val spark = Helper.getSparkSession()

  val user_animeDF = Helper.readParquet(spark, "user_anime.parquet")

  // Gets rid of interactions without reviews and irrelevant columns like progress and saves it in a parquet file
  val user_anime_reviewDF = user_animeDF.select("*")
    .where(col("review_id").isNotNull)
    .drop("last_interaction_date", "progress")

  user_anime_reviewDF.write.mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/user_anime_review.parquet")

  // Gets rid of irrelevant columns and saves it in a parquet file in the user file
  val userDF = Helper.readCSV(spark, "user.csv")

  userDF.drop("user_url", "last_online_date", "clubs")
    .write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/user.parquet")

  //Gets rid of irrelevant columns and saves it in a parquet file in the anime file
  val animeDF = Helper.readCSV(spark, "anime.csv")

  animeDF.select("anime_id", "type", "source_type", "num_episodes", "status", "start_date", "end_date", "genres", "score", "score_count", "popularity_rank", "favorites_count", "total_count")
    .write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/anime.parquet")


}
