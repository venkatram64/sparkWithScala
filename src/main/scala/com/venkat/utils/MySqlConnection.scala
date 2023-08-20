package com.venkat.utils

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

import java.util.Properties

object MySqlConnection {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def fetchData(tableName: String, user: String, password: String): Option[DataFrame] = {
    logger.info("Fetching the data for given configuration using options")
    val url = "jdbc:mysql://localhost:3306/ema_db"
    try{
      val df = SparkUtils.createSparkSession().read
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/ema_db")
        .option("dbtable", tableName)
        .option("user", user)
        .option("password", password)
        .load()
      Some(df)
    }catch {
      case e: Exception =>
      logger.error("exception occurred")
      e.printStackTrace()
      None
    }
  }

  def fetchData2(tableName: String, user: String, password: String): Option[DataFrame] = {
    logger.info("Fetching the data for given configuration ")
    try {
      val props = new Properties();
      props.put("user", user)
      props.put("password", password);
      val url = "jdbc:mysql://localhost:3306/ema_db"
      val df = SparkUtils.createSparkSession()
        .read
        .jdbc(url, tableName, props)
      Some(df)
    }catch {
      case e: Exception =>
        logger.error("exception occurred")
        e.printStackTrace()
        None
    }
  }

  def writeDFToMySqlTable(df: DataFrame, tableName: String): Unit = {
    try{
      df.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/ema_db?useSSL=false")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", "root")
        .option("password", "root")
        .option("dbtable", tableName)
        .save()
    }catch {
      case e: Exception =>
      logger.error("Exception thrown ")
      e.printStackTrace()
    }
  }

}
