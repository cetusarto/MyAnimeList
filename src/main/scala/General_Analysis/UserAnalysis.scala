package General_Analysis

import Helper.Helper

object UserAnalysis extends App {

  val spark = Helper.getSparkSession

  val userDF = Helper.readParquet(spark,"user.parquet")

  //Average and STDs of users
  userDF.selectExpr("avg(mean_score) as mean_score", "std(mean_score) as std_score",
    "avg(num_completed) as mean_completed", "std(num_completed) as std_completed",
    "avg(num_watching) as mean_watching", "std(num_watching) as std_watching",
    "avg(num_days) as mean_days", "std(num_days) as std_days").show()

}
