package General_Analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import Helper.Helper

object AnimeAnalysis extends App {

  val spark = Helper.getSparkSession
  val animeDF = Helper.readParquet(spark,"anime.parquet")
    .select("anime_id", "source_type", "num_episodes", "status", "genres")

  val fullCount = animeDF.count()

  animeDF.show()

  //Anime status
  animeDF.groupBy("status")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100).show()

  //Source type
  animeDF.groupBy("source_type")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100).show()

  //Genre
  animeDF.select(explode(split(col("genres"), "\\|")).as("genre"))
    .groupBy("genre").agg(count("*").as("count")).orderBy(desc("count")).show()

}
