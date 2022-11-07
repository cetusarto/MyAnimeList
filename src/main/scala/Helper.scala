package Helper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Commonly used methods to avoid Boilerplate *
 */

object Helper {

  def getSparkSession(app_name: String = "App"): SparkSession = SparkSession.builder()
    .appName(app_name)
    .config("spark.master", "local[*]")
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

  def readResult(spark: SparkSession, file: String) = spark.read
    .format("csv").options(Map(
    "mode" -> "failFast",
    "inferSchema" -> "true",
    "sep" -> ",",
    "header" -> "true",
    "path" -> file
  )).load()


  def readParquet(spark: SparkSession, file: String) = spark.read
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> s"src/main/resources/data/$file"
    )).load()

  def readParquetSchema(spark: SparkSession, file: String, schema: StructType) = spark.read
    .schema(schema)
    .options(Map(
      "mode" -> "failFast",
      "header" -> "true",
      "path" -> s"src/main/resources/data/$file"
    )).load()
}
