package org.duong.assignment


import java.io.{BufferedReader, InputStreamReader}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * This app is a Spark driver to perform data transformation on a raw input.
  *
  */
object CallHistoryTransformerApp {

  val MANDATORY_FIELDS_NUM: Int = 4

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)

    if (args.length < 2) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: CallHistoryTransformerApp <raw-input-path> <output-path>")
      System.exit(1)
    }

    val jobName = "CallHistoryTransformerApp"

    val conf = new SparkConf().setAppName(jobName)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> input \"" + inputPath + "\"")

    // REQ: CDR data are provided in CSV format but it can be different in separated character.
    // List out all files  and guest delimiter (CSV) of each.
    val filesAndDelimiter = sc.binaryFiles(inputPath).map {
      case (fileName, stream) => {
        val is = stream.open()
        val firstLine = new BufferedReader(new InputStreamReader(is)).readLine()
        (stream.getPath(), guessDelimiter(firstLine))
      }
    }.collect()

    val dataFrames = filesAndDelimiter.map {
      case (file, delimiter) => spark.read.option("header", "true").option("delimiter", delimiter).csv(file)
    }.map(x => normalizeSchema(x))

    // Union all these data frames into one big, perform data cleaning, formating,
    // then drop duplicate, on the set of unique key columns, Let's assume: FROM_PHONE_NUMBER, TO_PHONE_NUMBER, START_TIME
    val allInOne = dataFrames.reduce((x, y) => x.union(y))
      // cleaning data
      .filter(r => isValidRow(r))
      // Formatting data
      .map(r => formatRow(r))
      // dedup in place
      .dropDuplicates("FROM_PHONE_NUMBER", "TO_PHONE_NUMBER", "START_TIME")

    allInOne.write.option("header", "true").csv(outputPath)

    /**
      * format consolidate
      */
    def normalizeSchema(df: sql.DataFrame): sql.DataFrame = {
      // REQ: CDR data are provided in CSV format but it can be different in number of columns, columns’ name
      // standardize columns names by renaming variants into standard names
      // Standard column names is: FROM_PHONE_NUMBER;TO_PHONE_NUMBER;START_TIME;CALL_DURATION;IMEI;LOCATION
      df.withColumnRenamed("FROM_NUMBER", "FROM_PHONE_NUMBER")
        .withColumnRenamed("TO_NUMBER", "TO_PHONE_NUMBER")
        .withColumnRenamed("DURATION", "CALL_DURATION")
        // TODO more columns name variant corrections.

        // standardize format and column types.
        .select($"FROM_PHONE_NUMBER",
                $"TO_PHONE_NUMBER",
                to_timestamp($"START_TIME", "dd/MM/yyyy HH:mm:ss").as("START_TIME"),
                $"CALL_DURATION".cast(IntegerType), $"IMEI", $"LOCATION")
    }
  }

  def guessDelimiter(headerLine: String): String = {
    if (headerLine.count(_ == ',') > MANDATORY_FIELDS_NUM) ","
    else if (headerLine.count(_ == ';') > MANDATORY_FIELDS_NUM) ";"
    else if (headerLine.count(_ == '-') > MANDATORY_FIELDS_NUM) "-"
    else if (headerLine.count(_ == '|') > MANDATORY_FIELDS_NUM) "|"
    else throw new IllegalArgumentException("Cannot guest delimiter of the header line: " + headerLine)
  }

  def formatRow(r: Row): Row = {
    // TODO: re-formate the data, e.g. check for different date format variants, and coslidate into one standard format
    r
  }

  def isValidRow(r: Row): Boolean = {
    // TODO: check if row can be use, otherwise it will be filtered out.
    true
  }
}
