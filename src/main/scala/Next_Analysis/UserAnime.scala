package Next_Analysis

import Helper.Helper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object UserAnime extends App {
  val spark = Helper.getSparkSession("Fourth question")


  val userAnimeDF = Helper.readParquet(spark, "user_anime.parquet")




}
