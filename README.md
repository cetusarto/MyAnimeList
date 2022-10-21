# MyAnimeList Dataset Analysis using Spark with Scala #

This is a learning project for a Scala Spark for Big Data Course.
Its objective is getting insights from the users of the Anime and Manga social network MyAnimeList, using a dataset that contains data from users, anime shows and interactions between them.

![](C:\Users\cesar\Desktop\Cesar\Programming\Spark\Project Test\DB Test\img\unnamed.png)
The overview of the dataset can be found ond the [Kaggle Dataset](https://www.kaggle.com/datasets/svanoo/myanimelist-dataset). This project uses version 2 of this dataset.
It is also worth noting that this project is meant to run locally.


## SparkSession configuration ##
The next snippet shows the configuration used for the SparkSession. 
The only changed configuration is the reduction of shuffle partitions as there are not enough executors available (local cores) to take advantage of parallelization.

```scala
  val spark = SparkSession.builder()
  .appName("App Name")
  .config("spark.master", "local")
  .config("spark.sql.shuffle.partitions", "5")
  .getOrCreate()
```

## Project parts ##
The steps followed to achieve the final conclusions are divided in each of the following project parts

### Data Preparation ###
This folder contains two scripts:
1. Unifying Dataset: Takes the 70 raw csv files of user_anime schema and unifies them in a single parquet file for easier access in the next steps
2. Cleansing Dataset: Takes the previous parquet file and gets rid of the interactions with no review (users can set a show status without adding a review), and stores it in another parquet file. Both of this files are then used.

### Data Analysis ###