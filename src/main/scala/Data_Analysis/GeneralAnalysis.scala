package Data_Analysis

import org.apache.spark.sql.SparkSession

object GeneralAnalysis extends App {
  val spark = SparkSession.builder()
    .appName("General Analysis")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val user_anime_reviewDF = spark.read
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "sep" -> "\t",
      "header" -> "true",
      "path" -> s"src/main/resources/data/user_anime.parquet"
    )).load()




}
