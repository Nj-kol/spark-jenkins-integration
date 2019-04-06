package com.njkol.analytics

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.njkol.analytics.TopGenresByRatingsHelper._

import org.apache.logging.log4j.scala.Logging

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/16
 * @version 1.0.0
 *
 * Spark application to find top 10 genres of TV shows by ratings
 */
class TopGenresByRatings(hiveContext: HiveContext) extends Logging {

  import hiveContext.implicits._

  /**
   * Finds top N genres of TV shows by ratings
   *
   * @param animeData anime data
   * @param ratingData ratings data
   * @param typ Type of content ( TV, Movies etc )
   * @param noOfEpisodes Number of episodes to filter by
   * @param uptoRanks Ranks to see ( Ex - top 10 )
   * @return A DataFrame containing result
   */
  def getTopNGenreByRatings(animeData: DataFrame, ratingData: DataFrame, typ: String, noOfEpisodes: String, uptoRanks: String): DataFrame = {

    logger.info("Tokenize the multi-valued genre values by comma")
    val tokenizedGenre = animeData.withColumn("genre", tokenizerUDF($"genre", lit(",")))

    logger.info("De-normalize the genres")
    val deNormAnimeData = tokenizedGenre.withColumn("genre", explode($"genre")).withColumn("genre", trim($"genre"))
    deNormAnimeData.registerTempTable("anime_data")

    ratingData.registerTempTable("user_ratings")

    logger.info("Find the mean rating across all users")
    val meanRating = ratingData.selectExpr("round(mean(rating),2) as mean_rating").collect()(0)
      .getAs[Double]("mean_rating")

    logger.info("Join Anime data with user ratings")
    val joinedData = hiveContext.sql(animeToUserRatingSQL(typ, noOfEpisodes))

    logger.info(""" Group data by genres and find the mean user rating across each group.
       Then count the votes across each genre group """)
    val avgUsrRatings = joinedData.groupBy("genre")
      .agg(mean("user_rating").as("mean_user_rating"), count("user_id").as("votes_by_genre"))
      .withColumn("mean_user_rating", round($"mean_user_rating", 2))

    logger.info("Find the Bayesian weighted average rating using IMDB top 250 formula")
    val wtRatings = avgUsrRatings.withColumn("wt_rating", round(weightedRatingUDF($"mean_user_rating", $"votes_by_genre", lit(1000), lit(meanRating)), 2))

    logger.info("Order data in descending order by the weighted rating")
    val spec = Window.orderBy($"wt_rating".desc)

    logger.info("Rank the data up to N ( from input specified )")
    wtRatings.withColumn("rank", row_number().over(spec))
      .where($"rank" <= uptoRanks.toInt)
      .drop("mean_user_rating").drop("votes_by_genre")
      .drop("wt_rating")
  }
}

