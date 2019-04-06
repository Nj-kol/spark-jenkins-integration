package com.njkol.analytics

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.scalatest._
import org.apache.spark.sql.hive.HiveContext

import org.apache.logging.log4j.scala.Logging

trait EnvironmentInitializerSC extends WordSpec with Matchers with BeforeAndAfterAll with Logging {

  private val master = "local[2]"
  private val appName = "top-ten-spark"

  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var hiveContext: HiveContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = SparkContext.getOrCreate(conf)
    sqlContext = new SQLContext(sc)
    hiveContext = new HiveContext(sc)
  }

  override def afterAll() {

    if (sc != null) {
      sc.stop()
    }
  }
}
