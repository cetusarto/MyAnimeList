# MyAnimeList Dataset Analysis using Spark with Scala #

This is a learning project for a Scala Spark for Big Data Course.
Its objective is getting insights from the users of the Anime and Manga social network MyAnimeList, using a dataset that contains data from users, anime shows and interactions between them.

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