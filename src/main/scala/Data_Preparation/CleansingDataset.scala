package Data_Preparation

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * Uses Spark SQL functions to get rid of all interactions without review and saves it in a parquet file
 * */

object CleansingDataset extends App {
  val spark = SparkSession.builder()
    .appName("Cleansing Dataset")
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

  val user_anime_reviewDF = user_animeDF.select("*")
    .where(col("review_id").isNotNull)
    .drop("last_interaction_date", "progress")

  user_anime_reviewDF.write.mode(SaveMode.Overwrite)
    .save("src/main/resources/data/user_anime_review.parquet")

}
