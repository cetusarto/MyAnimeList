package First_Cleansing

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Uses Spark SQL functions to clean data
 * */

object CleansingDataset {
  val spark = SparkSession.builder()
    .appName("Cleansing and creating format")
    .config("spark.master", "local")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "5")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()


  val user_animeDF = spark.read
      .format("csv").options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "sep" -> "\t",
      "header" -> "true",
      "path" -> s"src/main/resources/data/user_anime.parquet"
    )).load().cache()



}
