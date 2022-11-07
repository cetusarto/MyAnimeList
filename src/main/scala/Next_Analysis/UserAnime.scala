package Next_Analysis

import Helper._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object UserAnime extends App {
  val spark = Helper.getSparkSession("UserAnime")

  val userAnimeDF = Helper.readParquetSchema(spark, "user_anime.parquet", SchemaHelper.getUserAnimeSchema)

  /**
   * Correlation analysis
   */
  //    val userAnimeReviewsDF = userAnimeDF
  //      .select("score", "review_score", "review_story_score",
  //        "review_animation_score", "review_sound_score", "review_character_score", "review_enjoyment_score", "review_id")
  //      .where(col("review_id").isNotNull)
  //
  //    val subReviews = Array("review_score", "score","review_story_score", "review_animation_score", "review_sound_score", "review_character_score", "review_enjoyment_score")
  //    val correlations = for {i <- 0 to 6; j <- i to 6 if i != j} yield (subReviews(i) , subReviews(j) , userAnimeReviewsDF.stat.corr(subReviews(i), subReviews(j)))
  //    import spark.implicits._
  //    correlations.toDF("ReviewA","ReviewB","Correlation").orderBy(desc("Correlation")).show(30)

  /**
   * Why users want to leave reviews?
   */
  //
  val userAnime2DF = userAnimeDF
    .select("user_id", "score", "review_score", "review_id")
    .where(col("score").isNotNull or col("review_id").isNotNull)
    .withColumn("hasReview", col("review_id").isNotNull)

  val withReviewDF =
    userAnime2DF
      .where(col("hasReview"))
      .groupBy("user_id")
      .agg(
        avg("score").as("average_score_WReview"))

  val withoutReviewDF =
    userAnime2DF
      .where(not(col("hasReview")))
      .groupBy("user_id")
      .agg(
        avg("score").as("average_score"))

  withoutReviewDF.join(withReviewDF, "user_id")
    .selectExpr("user_id",
      "(average_score_WReview - average_score) as B",
      "(average_score_WReview) as C",
      "(average_score) as E"
    ).select(
    avg("E").as("average_score"),
    avg("C").as("AvgScoreWReview"),
    avg("B").as("AVG(average_score_WReview - average_score)"),
    stddev("B").as("STD(average_score_WReview - average_score)")
  ).show()


}