package Next_Analysis

import Helper.Helper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object UserAnime extends App {
  val spark = Helper.getSparkSession("UserAnime")

  val userAnimeDF = Helper.readParquet(spark, "user_anime.parquet")

  /**
   * Correlation analysis
   */
    val userAnimeReviewsDF = userAnimeDF
      .select("score", "review_score", "review_story_score",
        "review_animation_score", "review_sound_score", "review_character_score", "review_enjoyment_score", "review_id")
      .where(col("review_id").isNotNull)

    val subReviews = Array("review_score", "score","review_story_score", "review_animation_score", "review_sound_score", "review_character_score", "review_enjoyment_score")
    val correlations = for {i <- 0 to 5; j <- i to 5 if i != j} yield (subReviews(i) , subReviews(j) , userAnimeReviewsDF.stat.corr(subReviews(i), subReviews(j)))
    import spark.implicits._
    correlations.toDF("ReviewA","ReviewB","Correlation").orderBy(desc("Correlation")).show()

  /**
   * Why users want to leave reviews?
   */

  val userAnime2DF = userAnimeDF
    .select("user_id", "score", "review_score", "review_id")
    .where(col("score").isNotNull or col("review_id").isNotNull)
    .withColumn("hasReview", col("review_id").isNotNull)

  val withReviewDF =
    userAnime2DF
      .where(col("hasReview"))
      .groupBy("user_id")
      .agg(
        count("*").as("count_reviews"),
        avg("score").as("average_score_WReview"))

  val withoutReviewDF =
    userAnime2DF
      .where(not(col("hasReview")))
      .groupBy("user_id")
      .agg(
        avg("score").as("average_score"))

  withoutReviewDF.join(withReviewDF, "user_id").where("count_reviews > 1")
    .selectExpr("user_id",
      "(average_score_WReview - average_score) as B",
      "(average_score_WReview) as C",
      "(average_score_WReview) as E"
    ).select(
    avg("C").as("AvgReviewScore"),
    avg("E").as("AvgScoreWReview"),
    avg("B").as("AvgScoreWReview - AvgScoreWNoReview"),
    stddev("B").as("SDScoreWReview - SDScoreWNoReview")
  ).show()


}