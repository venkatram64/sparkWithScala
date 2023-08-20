package com.venkat

import com.venkat.utils.{MyJsonParser, MySqlConnection, SparkUtils}
import org.slf4j.LoggerFactory
//--add-exports java.base/sun.nio.ch=ALL-UNNAMED   ---> vmargs

object MyApp {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    println("Hello, Srijan")
    logger.info("Started main application")
    //mySparkEg()
    //fetchData2()
    //fetchData()
    createAndPopulateHiveTable()
    readeHiveTable()
    replaceNullValues()
    loadDataIntoMysql()
    writeToHiveTable()
  }

  def replaceNullValues(): Unit = {
    try {
      val spark = SparkUtils.createSparkSession()
      val empDataDF = spark.sql("select * from myemp.employee")
      val df = MySparkTranslate.replaceNullValues(empDataDF)
      df.show()
    } catch {
      case e: Exception =>
        logger.error("Unable to read the data")
        e.printStackTrace()
    }
  }

  private def mySparkEg(): Unit = {
    val spark = SparkUtils.createSparkSession()
    println("Spark Session created...")
    val sampleSeq = Seq((1, "Spark"), (2, "Big Data"))
    val df = spark.createDataFrame(sampleSeq)
      .toDF("Course Id", "Course Name")

    df.show()
    df.write
      .format("csv")
      .save("first_df")
  }

  def fetchData(): Unit = {
    logger.info("fetching the data...")
    try{
      val df = MySqlConnection.fetchData("employee", "root", "root")
      df.get.show()
    }catch {
      case e:Exception =>
        logger.error("An error occurred while fetching the data from table")
    }

  }

  def fetchData2():Unit = {
    logger.info("fetching the data...")
    try{
      val df = MySqlConnection.fetchData2("employee", "root", "root")
      df.get.show()
    }catch {
      case e:Exception =>
        logger.error("An error occurred while fetching the data from table")
    }
  }

  def createAndPopulateHiveTable(): Unit = {
    try {
      val spark = SparkUtils.createSparkSession()
      SparkUtils.createHiveTable(spark)
    } catch {
      case e: Exception =>
        logger.info("unable to create the table and populate")
        e.printStackTrace()
    }
  }

  def readeHiveTable(): Unit = {
    try {
      val spark = SparkUtils.createSparkSession()
      val empDF = SparkUtils.readHiveTable(spark)
      empDF.get.show()
    } catch {
      case e: Exception =>
        logger.info("unable to create the table and populate")
        e.printStackTrace()
    }
  }

  def loadDataIntoMysql(): Unit = {
    logger.info("loading data into mysql")
    try {
      val spark = SparkUtils.createSparkSession()
      val empDF = SparkUtils.readHiveTable(spark)
      val empDFDropID = empDF.get.drop("id")
      MySqlConnection.writeDFToMySqlTable(empDFDropID, MyJsonParser.parsePropsForTable())
    } catch {
      case e: Exception =>
        logger.info("unable to create the data into table and populate")
        e.printStackTrace()
    }
  }

  def writeToHiveTable() : Unit = {
    logger.info("writing data into hive table...")
    try{
      val spark = SparkUtils.createSparkSession()
      val empDF = SparkUtils.readHiveTable(spark)
      val newEmpDF = MySparkTranslate.replaceNullValues(empDF.get)
      val df = MySparkTranslate.writeToHiveTable(spark,newEmpDF,"my_hive_emp")
      logger.info("Data fetched from hive table after writing into ")
      df.get.show()
    }catch {
      case e: Exception =>
        logger.info("unable to write the data into table and populate")
        e.printStackTrace()
    }
  }

}

