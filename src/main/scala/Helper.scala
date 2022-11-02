package Helper

import org.apache.spark.sql.SparkSession

/**
 * Commonly used methods to avoid Boilerplate *
 */

object Helper{

  def getSparkSession: SparkSession = SparkSession.builder()
    .appName("Cleansing Dataset")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  def readCSV(spark: SparkSession, file: String) = spark.read
    .format("csv").options(Map(
    "mode" -> "failFast",
    "inferSchema" -> "true",
    "sep" -> "\t",
    "header" -> "true",
    "path" -> s"src/main/resources/data/$file"
  )).load()


  def readParquet(spark: SparkSession, file:String)= spark.read
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> s"src/main/resources/data/$file"
    )).load()


}