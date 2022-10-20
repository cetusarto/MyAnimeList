package First_Cleansing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * Unifies all 69 csv files in one big parquet file for faster reading
**/


object UnifyingDataset extends App {
  val spark = SparkSession.builder()
    .appName("Cleansing and creating format")
    .config("spark.master", "local")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "5")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()



  //Returns a user_anime DF
  def getUser_AnimeDF(edition: String) = {
    spark.read
      .format("csv").options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "sep" -> "\t",
      "header" -> "true",
      "path" -> s"src/main/resources/data/user_anime0000000000$edition.csv"
    )).load()
  }

  //Create an array of dataframes
  var DFArray = collection.mutable.ArrayBuffer.empty[DataFrame]

  //Stores al 70 DFs into the array
  var numbers = for (i <- 0 to 69) yield f"${i}%02d"
  for (i <- numbers) DFArray += getUser_AnimeDF(i)

  //Unifies one big DF uniting every dataframe
  val FinalArray = DFArray.reduce(_ union _)

  //Writes the dataframe
  FinalArray.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/user_anime.parquet")

}
