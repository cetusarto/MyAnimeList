package Helper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
 * Schemas
 */

object SchemaHelper {

  def getUserSchema = StructType(Array(
    StructField("user_id", StringType),
    StructField("num_completed", IntegerType),
    StructField("num_watching", IntegerType),
    StructField("num_on_hold", IntegerType),
    StructField("num_dropped", IntegerType),
    StructField("num_plan_to_watch", IntegerType),
    StructField("num_days", DoubleType),
    StructField("mean_score", DoubleType)
  ))

  def getAnimeSchema = StructType(Array(
    StructField("anime_id", IntegerType),
    StructField("type", StringType),
    StructField("source_type", StringType),
    StructField("status", StringType),
    StructField("start_date", StringType),
    StructField("end_date", StringType),
    StructField("genres", StringType),
    StructField("favorites_count", IntegerType)
  ))

  def getUserAnimeSchema =
    StructType(Array(
      StructField("user_id", StringType),
      StructField("anime_id", IntegerType),
      StructField("favorite", IntegerType),
      StructField("review_id", IntegerType),
      StructField("review_date", StringType),
      StructField("review_num_useful", IntegerType),
      StructField("review_score", IntegerType),
      StructField("review_story_score", IntegerType),
      StructField("review_animation_score", IntegerType),
      StructField("review_sound_score", IntegerType),
      StructField("review_character_score", IntegerType),
      StructField("review_enjoyment_score", IntegerType),
      StructField("score", IntegerType),
      StructField("status", StringType),
      StructField("progress", IntegerType),
      StructField("last_interaction_date", StringType)
    ))


}