package com.njkol.analytics

import org.scalatest._
import org.apache.spark.sql.types.{ StringType, StructField, StructType, DoubleType, LongType, IntegerType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{ udf, explode, lit, round }

import org.apache.spark.sql.hive.HiveContext


/**
 * @author Nilanjan Sarkar
 * @since  2017/08/17
 * @version 1.0.0
 *
 * Tester application for : TopGenresByRatings
 */
class TopGenresByRatingsTest extends EnvironmentInitializerSC {

  "Test Tokenizer UDF" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expected = Array(Row("A"), Row("B"), Row("C"), Row("D"), Row("E"), Row("F"))

    val schema = StructType(Seq(StructField("genre", StringType, nullable = true)))

    val testData = Seq(Row("A,B,C"), Row("D,E,F"))
    val rddTest = sc.parallelize(testData)
    val testDF = sqlCtx.createDataFrame(rddTest, schema)
    val result = testDF.withColumn("genre", explode(TopGenresByRatingsHelper.tokenizerUDF($"genre", lit(","))))
    val actual = result.collect

    expected shouldBe actual
  }

  "Test Weighted Average UDF" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val expected = Array(Row("6.66", "145000", 6.66), Row("7.81", "81060", 7.8), Row("5.58", "79060", 5.6))

    val inputDataschema = StructType(Seq(StructField("mean_user_rating", StringType, nullable = true), StructField("votes_by_genre", StringType, nullable = true)))

    val finalSchema = StructType(Seq(
      StructField("mean_user_rating", StringType, nullable = true),
      StructField("votes_by_genre", StringType, nullable = true),
      StructField("wt_rating", DoubleType, nullable = true)))

    val testData = Seq(Row("6.66", "145000"), Row("7.81", "81060"), Row("5.58", "79060"))
    val rddTest = sc.parallelize(testData)
    val testDF = sqlCtx.createDataFrame(rddTest, inputDataschema)

    val result = testDF.withColumn("wt_rating", round(TopGenresByRatingsHelper.weightedRatingUDF($"mean_user_rating", $"votes_by_genre", lit(1000), lit(7.0)), 2))
    val actual = result.collect

    expected shouldBe actual
  }

  "Top 10 genre by ratings test" in {

    val sqlCtx = hiveContext
    import sqlCtx.implicits._

    val schema = StructType(Array(StructField("genre", StringType, nullable = true), StructField("rank", IntegerType, nullable = true)))

    // prepare test data
    val expected = Array(
      Row("Thriller", 1),
      Row("Police", 2),
      Row("Military", 3),
      Row("Psychological", 4),
      Row("Dementia", 5),
      Row("Josei", 6),
      Row("Historical", 7),
      Row("Drama", 8),
      Row("Mystery", 9),
      Row("Samurai", 10))

    // run the actual code with data provided
    val animePath = "src/test/resources/data/anime.csv"
    val ratingPath = "src/test/resources/data/rating.csv.gz"

    logger.info("Loading anime data")
    val animeData = hiveContext.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true")
      .load(animePath)

    logger.info("Loading ratings data")
    val ratingData = hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ratingPath)
      
    val typ = "TV"
    val noOfEpisodes = "10"
    val ranks = "10"

    val algoClass = new TopGenresByRatings(hiveContext)
    val rankData = algoClass.getTopNGenreByRatings(animeData, ratingData, typ, noOfEpisodes, ranks)
    val actual = rankData.collect
    expected shouldBe actual

    // Show the output
    rankData.show(false)
  }
}