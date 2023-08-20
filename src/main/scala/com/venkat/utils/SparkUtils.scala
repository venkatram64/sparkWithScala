package com.venkat.utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.nio.file.Paths
//https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
//"file:///D:/winutils"
object SparkUtils {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): SparkSession = {

    logger.info("Creating SparkSession ")
    val winutilsPath = Paths.get("D:/winutils").toAbsolutePath().toString()
    System.setProperty("hadoop.home.dir", winutilsPath)
    var spark = SparkSession
      .builder()
      .appName("MySparkApp")
      .config("spark.master", "local[3]")
      .enableHiveSupport()
      .getOrCreate();
    spark.sparkContext.setLogLevel("WARN") //log level
    spark

  }

  def createHiveTable(spark: SparkSession): Unit = {
    logger.info("Creating hive table")

    spark.sql("create database if not exists myemp")

    //drop table first
    spark.sql("drop table if exists myemp.employee")

    spark.sql("create table if not exists " +
        "myemp.employee(id string, email string, first_name string, last_name string)")

    spark.sql("insert into myemp.employee values(1, 'john@test.com', 'John', 'Doe')")
    spark.sql("insert into myemp.employee values(2, 'jean@test.com', 'Jean', 'Doe')")
    spark.sql("insert into myemp.employee values(3, '', 'Mark', 'Clinton')")
    spark.sql("insert into myemp.employee values(4, 'test@test.com', '', 'Harper')")
    spark.sql("insert into myemp.employee values(5, '', 'Bob', 'Tyson')")
    spark.sql("insert into myemp.employee values(6, 'steve@test.com', 'Steve', '')")

    //set empty strings as null
    spark.sql("alter table myemp.employee set tblproperties('serialization.null.format'='')")
  }

  def readHiveTable(spark: SparkSession) : Option[DataFrame] = {
    try{
      val empDataDF = spark.sql("select * from myemp.employee")
      Some(empDataDF)
    }catch {
      case e: Exception =>
        logger.error("Unable to read the data")
        e.printStackTrace()
        None
    }
  }

}
