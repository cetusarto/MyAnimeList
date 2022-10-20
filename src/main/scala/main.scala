

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main extends App {
    val spark = SparkSession.builder()
      .appName("Data Sources and Formats")
      .config("spark.master", "local")
      .getOrCreate()

  spark.read.format("csv").options(Map(
    "mode" -> "failFast",
    "path" -> "src/main/resources/data/MyAnimeList/anime.csv",
    "inferSchema" -> "true")).load()


}