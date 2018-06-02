package org.toan.assignment

import java.io.IOException
import java.time.LocalDate

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.io.Path

/**
  * Create by Toan Le on 5/30/2018
  */
object FindActualActivationDateApp {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Only one parameter needed.")
      System.out.println("Example: FindActualActivationDateApp <path-to-input-file-data>")
      System.exit(1)
    }

    val appName = "FindActualActivationDate"

    val spark = SparkSession.builder.appName(appName).getOrCreate()

    System.out.println("AppLog: Start FindActualActivationDate App")

    // only used on local for testing
    val outputPath = "outputResults"
    deleteFolderIfExist(outputPath)

    // Read from file and convert to list activationlog
    val phoneActivationLogs = spark.read.option("header", "true")
      .csv(args(0)).rdd.map(r => getActivationLog(r))

    System.out.println("AppLog: Loading File successfully")

    // Partitions: Group by phone number
    val results = phoneActivationLogs.groupBy(_.phoneNumber)
      .mapValues(AlgorithmHelpers.getActualActivation)
    System.out.println("AppLog: Process Data successfully")

    // Write output
    val schema = StructType(
      StructField("PHONE_NUMBER", StringType, false) ::
        StructField("REAL_ACTIVATION_DATE", StringType, false) :: Nil)

    spark.createDataFrame(results.map(x => Row(x._1, x._2.toString())), schema)
      .write.option("header", "true").csv(outputPath)

    System.out.println("AppLog: 'save to file' successfully")
  }

  private def deleteFolderIfExist(folderPathStr: String): Unit ={
    val outputPath = Path(folderPathStr)
    try {
      outputPath.deleteRecursively()
    } catch {
      case e: IOException => println("some file could not be deleted")
    }
  }

  private def getActivationLog(activationLog: Row): ActivationLog = {
    val phoneNumber = activationLog.getString(0)
    val activateDate = LocalDate.parse(activationLog.getString(1))
    val deActivateDate = Option(activationLog.getString(2)) match {
      case Some(x) if !x.isEmpty => Some(LocalDate.parse(x))
      case _ => None
    }

    ActivationLog(phoneNumber, activateDate, deActivateDate)
  }
}
