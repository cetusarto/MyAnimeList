# MyAnimeList Dataset Analysis using Spark with Scala #

This is a learning project for a Scala Spark for Big Data Course.
Its objective is getting insights from the users of the Anime and Manga social network MyAnimeList, using a dataset that contains data from users, anime shows and interactions between them.

![title](img/unnamed.png)

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

## Main Questions ##
This project tries to answer the following main questions from the dataset:
1. How does watch time affects user-anime interactions?
2. How does genre affect user-anime interactions?
3. How does age of the anime affect user-anime interactions?
4. How does popularity of the anime affect user-anime interactions?
5. What makes a user to leave a review?

## Project parts ##
The steps followed to achieve the final conclusions are divided in each of the following project parts

### Helper object ###
To avoid boilerplate the Helper object is built. It offers a series of methods for every class to use for the repetitive taks like the SparkSession builder and the reading of files.

### Data Preparation ###
This part contains two notebooks:
1. Unifying Dataset: Takes the 70 raw csv files of user_anime schema and unifies them in a single parquet file for easier access in the next steps
2. Cleansing Dataset: Deletes useless columns and saves them in a parquet file in order to optimize the reading process of each notebook.

### General Analysis ###
This part analyses the basic indicators of every data table:
1. Anime: This notebook gives the following conclusions:
- The majority of the anime has finished airing
```
+----------------+-----+-----------------+
|          status|count|       percentage|
+----------------+-----+-----------------+
| Finished Airing|12755|95.33597428806338|
|Currently Airing|  258|1.928395246281486|
|   Not yet aired|  366|2.735630465655131|
+----------------+-----+-----------------+
```
- The most important sources are Original and manga
```
+------------+-----+-------------------+
| source_type|count|         percentage|
+------------+-----+-------------------+
|    Original| 4507|  33.68712160849092|
|   Web manga|  285|  2.130204051124897|
|       Novel|  588|  4.394947305478735|
|       Music|   27| 0.2018088048434113|
|        Book|  120| 0.8969280215262726|
|   Web novel|    6|0.04484640107631362|
|4-koma manga|  291|  2.175050452201211|
|       Manga| 4083| 30.517975932431423|
|   Card game|   61|0.45593841094252185|
|       Other|  608|  4.544435309066448|
| Light novel|  833|  6.226175349428209|
|        Game|  906|  6.771806562523358|
|Visual novel| 1045|  7.810748187457957|
| Mixed media|   19| 0.1420136034083265|
+------------+-----+-------------------+
```
- The count of anime of each genre (44 genres in total that can be seen in the results folder) 
```
+-------------+-----+
|        genre|count|
+-------------+-----+
|       Comedy| 5091|
|       Action| 3495|
|      Fantasy| 2785|
|    Adventure| 2379|
|        Drama| 2146|
|       Sci-Fi| 2087|
|      Romance| 1772|
+-------------+-----+
```
2. User: A short summary of the most interesting columns is made and gives the following results:

```
+-------+------------------+------------------+------------------+------------------+
|summary|        mean_score|     num_completed|      num_watching|          num_days|
+-------+------------------+------------------+------------------+------------------+
|  count|           1123284|           1123284|           1123284|           1123284|
|   mean| 7.122094750748184|159.08479155761142|11.487242763183666| 55.73767017067854|
| stddev|2.4771154476423756| 228.7494619791284|35.294089253672325|134.43298319025172|
|    max|              10.0|             18659|             11014|          105338.6|
+-------+------------------+------------------+------------------+------------------+
```
It shows big differences in the latter attributes, where the standard  deviation shows how dispersed the behavior of the users is.
3. UserAnime: With this notebook it is found that nearly half of the interactions do not have scores.  
```
+---------+---------+------------------+
|has_score|    count|        percentage|
+---------+---------+------------------+
|    false| 94596066|42.265743788685654|
|     true|129216548| 57.73425621131435|
+---------+---------+------------------+
```
It is shown that besides the application not letting users score without having watched the anime, a big group of users that have completed it, simply do not leave their score. 
```
+-------------+--------+------------------+
|       status|   count|        percentage|
+-------------+--------+------------------+
|     watching| 4770335| 5.042847130661861|
|    completed|27334555|28.896080097030673|
|      dropped| 3023378| 3.196092742376834|
|      on_hold| 3010012|3.1819631907314205|
|plan_to_watch|56457786| 59.68301683919921|
+-------------+--------+------------------+
```
While the other 58% contains mainly completed interactions.
```
+---------+---------+------------------+
|   status|    count|        percentage|
+---------+---------+------------------+
|completed|122194112| 94.56537408815471|
| watching|  2330148|1.8032891576704246|
|  dropped|  3217123|2.4897143978803706|
|  on_hold|  1475165|1.1416223562944896|
+---------+---------+------------------+
```
With the previous analysis and knowledge of the dataset, the main questions are set and the Next Analysis is made.

### Next Analysis ###



## Conclusions ##

## Future implementations ##