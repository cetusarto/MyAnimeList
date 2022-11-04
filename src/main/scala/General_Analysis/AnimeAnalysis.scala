package General_Analysis

import org.apache.spark.sql.functions._
import Helper._
import org.apache.spark.sql.SaveMode

object AnimeAnalysis extends App {

  val spark = Helper.getSparkSession("AnimeAnalysis")
  val animeDF = Helper.readParquetSchema(spark, "anime.parquet",SchemaHelper.getAnimeSchema)
    .select("anime_id", "source_type", "status", "genres")

  val fullCount = animeDF.count()

  //Anime status
  animeDF.groupBy("status")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100)
  .show()

  //Source type
  animeDF.groupBy("source_type")
    .agg(count("*").as("count"))
    .withColumn("percentage", col("count") / fullCount * 100).show()

  //Genre
  animeDF.select(explode(split(col("genres"), "\\|")).as("genre"))
    .groupBy("genre").agg(count("*")
    .as("count")).orderBy(desc("count")).coalesce(1)
    .show()

  //genres.write.format("csv").mode(SaveMode.Overwrite).save("src/main/resources/data/genres.csv")

}
