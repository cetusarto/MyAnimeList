package General_Analysis

import Helper._

object UserAnalysis extends App {

  val spark = Helper.getSparkSession("UserAnalysis")

  val userDF = Helper.readParquetSchema(spark,"user.parquet",SchemaHelper.getUserSchema)

  //Average and STDs of users

  userDF.select("mean_score","num_completed","num_watching","num_days")
    .summary("count","mean","stddev","max").show()

}
