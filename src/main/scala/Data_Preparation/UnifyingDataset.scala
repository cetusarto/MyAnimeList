package Data_Preparation

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import Helper.Helper


/**
 * Unifies all 69 csv files in one big parquet file for faster reading
 * */


object UnifyingDataset extends App {

  val spark = Helper.getSparkSession()
  def getUser_AnimeDF(edition: String) = Helper.readCSV(spark, s"user_anime0000000000$edition.csv")


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
