package General_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AnimeAnalysis extends App {

  val spark = SparkSession.builder()
    .appName("Anime Analysis")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val animeDF = spark.read
    .format("csv")
    .options(Map(
      "sep" -> "\t",
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/data/anime.csv"
    )).load().select("anime_id", "source_type", "num_episodes", "status", "genres")

  val fullCount = animeDF.count()

  animeDF.show()

  //Anime status
  animeDF.groupBy("status")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100).show()

  //Source type
  animeDF.groupBy("source_type")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100).show()

  //Genre
  animeDF.select(explode(split(col("genres"), "\\|")).as("genre"))
    .groupBy("genre").agg(count("*").as("count")).orderBy(desc("count")).show()

}
