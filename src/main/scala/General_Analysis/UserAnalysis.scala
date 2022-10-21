package General_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserAnalysis extends App {

  val spark = SparkSession.builder()
    .appName("User Analysis")
    .config("spark.master", "local")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val userDF = spark.read
    .format("csv")
    .options(Map(
      "sep" -> "\t",
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "header" -> "true",
      "path" -> "src/main/resources/data/user.csv"
    )).load()

  //Average and STDs of users
  userDF.selectExpr("avg(mean_score) as mean_score", "std(mean_score) as std_score",
    "avg(num_completed) as mean_completed", "std(num_completed) as std_completed",
    "avg(num_watching) as mean_watching", "std(num_watching) as std_watching",
    "avg(num_days) as mean_days", "std(num_days) as std_days").show()

}
