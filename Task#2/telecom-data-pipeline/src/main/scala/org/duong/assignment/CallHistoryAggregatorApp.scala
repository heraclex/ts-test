package org.duong.assignment

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * This app is a Spark driver to perform data transformation on a raw input.
  *
  */
object CallHistoryAggregatorApp {


  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)

    if (args.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: CallHistoryAggregatorApp <input-path> <output-path>")
      System.exit(1)
    }

    val jobName = "CallHistoryAggregatorApp"

    val conf = new SparkConf().setAppName(jobName)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    val inputPath = args(0)
    val outputPath = args(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> input \"" + inputPath + "\"")

    // Read the input
    val df = spark.read.option("header", "true").csv(inputPath).select($"FROM_PHONE_NUMBER",
        $"TO_PHONE_NUMBER",
        to_timestamp($"START_TIME", "dd/MM/yyyy HH:mm:ss").as("START_TIME"),
        $"CALL_DURATION".cast(IntegerType), $"IMEI", $"LOCATION")

    // For each number we want to have:
    val aggsSimple = df.groupBy($"FROM_PHONE_NUMBER").agg(
      //Number of call, total call duration.
      count($"*").alias("NUMBER_OF_CALLS"),
      sum($"CALL_DURATION").alias("TOTAL_CALL_DURATION"),
      //Number of call in working hour (8am to 5pm).
      count(
        when(hour($"START_TIME").between(8, 17), true)
      ).alias("NUMBER_OF_CALLS_IN_WORKING_HOURS")
    )

    // For each number we want to have:
    //   Find the IMEI which make most call.
    val countByImei = df.groupBy($"FROM_PHONE_NUMBER", $"IMEI").agg(count($"*").alias("NUMBER_OF_CALLS")).alias("counts")
    val maxCalls = countByImei.groupBy($"FROM_PHONE_NUMBER").agg(max($"NUMBER_OF_CALLS").as("MAX_CALLS")).alias("maxs")
    val imeiWithMostCalls = countByImei.join(maxCalls,
      ($"NUMBER_OF_CALLS" === $"MAX_CALLS") && ($"counts.FROM_PHONE_NUMBER" === $"maxs.FROM_PHONE_NUMBER")).
      select($"counts.FROM_PHONE_NUMBER", $"counts.IMEI".as("IMEI_MAKE_MOST_CALLS"))

    // For each number we want to have:
    //   Find top 2 locations which make most call.
    val countByLocation = df.groupBy($"FROM_PHONE_NUMBER", $"LOCATION").agg(count($"*").alias("NUMBER_OF_CALLS"))
    val top2Locations = countByLocation.groupByKey(r => r.getAs[String]("FROM_PHONE_NUMBER")).mapGroups((k, v) => {
      // Sort items desc by NUMBER_OF_CALLS
      val sorted = v.toList.sortWith((r1, r2) => r1.getAs[Long]("NUMBER_OF_CALLS") > r2.getAs[Long]("NUMBER_OF_CALLS"))
      val mostLocaltion = if (sorted.nonEmpty) sorted.head.getAs[String]("LOCATION") else null
      val secondMostLocaltion = if (sorted.tail.nonEmpty) sorted.tail.head.getAs[String]("LOCATION") else null
      (k, mostLocaltion, secondMostLocaltion)
    }).toDF("FROM_PHONE_NUMBER", "MOST_LOCATION", "SECOND_MOST_LOCATION")


    // terminate actions: store agg to hdfs
    aggsSimple.write.option("header", "true").csv(outputPath + "/simple-aggs")
    imeiWithMostCalls.write.option("header", "true").csv(outputPath + "/most-imei")
    top2Locations.write.option("header", "true").csv(outputPath + "/top-2-locations")
  }
}
