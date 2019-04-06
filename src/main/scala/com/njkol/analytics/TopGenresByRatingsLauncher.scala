package com.njkol.analytics

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import org.slf4j._
import org.apache.logging.log4j.scala.Logging

/**
 * @author Nilanjan Sarkar
 * @since  2017/08/16
 * @version 1.0.0
 *
 * Spark Launcher Application to find top N genres of TV shows by ratings
 */
object TopGenresByRatingsLauncher extends App with Logging {

  if (args.length < 5) {
    logger.error("Number of arguments are less. Required arguments are : [ANIME_DATA_PATH] [RATINS_DATA_PATH] [TYPE_OF_CONTENT] [NO_OF_EPISODES] [UPTO_RANK] [OUTPUT_PATH]")
    throw new IllegalArgumentException("Number of arguments are less. Required arguments are : [ANIME_DATA_PATH] [RATINS_DATA_PATH] [TYPE_OF_CONTENT] [NO_OF_EPISODES] [UPTO_RANK] [OUTPUT_PATH]")
  }

  private val animePath = args(0)
  private val ratingPath = args(1)
  private val typ = args(2)
  private val noOfEpisodes = args(3)
  private val ranks = args(4)
  private val outputPath = args(5)

  private val conf = new SparkConf().setAppName("TopGenresByRatings")
  private val sc = new SparkContext(conf)
  private val hiveContext = new HiveContext(sc)

  logger.info("Loading anime data")
  val animeData = hiveContext.read.format("com.databricks.spark.csv").option("header", "true")
    .option("inferSchema", "true")
    .load(animePath)

  logger.info("Loading ratings data")
  val ratingData = hiveContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(ratingPath)

  private val algoClass = new TopGenresByRatings(hiveContext)
  private val rankData = algoClass.getTopNGenreByRatings(animeData, ratingData, typ, noOfEpisodes, ranks)

  // Finally write the result
  rankData.write
    .format("com.databricks.spark.csv")
    .mode("overwrite").option("header", "true")
    .save(outputPath)

  logger.info("============================================================================================================================")
  logger.info(" ")
  logger.info(s"                         RESULTS STORED SUCCESSFULLY AT : ${outputPath}                                                    ")
  logger.info("============================================================================================================================")
}