package General_Analysis

import Helper.Helper

object UserAnalysis extends App {

  val spark = Helper.getSparkSession("UserAnalysis")

  val userDF = Helper.readParquet(spark,"user.parquet")

  //Average and STDs of users

  userDF.select("mean_score","num_completed","num_watching","num_days")
    .summary("count","mean","stddev","max").show()

}
