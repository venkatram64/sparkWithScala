package com.venkat

import com.venkat.utils.MyJsonParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object MySparkTranslate {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def replaceNullValues(df: DataFrame) :DataFrame = {
    logger.info("Transforming null values into Unknown")
    var transformDf: DataFrame = df

    if(MyJsonParser.getConfigValue("body.null_value_replacement.email") == "YES"){
      transformDf = df.na.fill(value ="Unknown", Seq("email"))
    }
    if(MyJsonParser.getConfigValue("body.null_value_replacement.first_name") == "YES"){
      transformDf = transformDf.na.fill(value ="Unknown", Seq("first_name"))
    }
    if (MyJsonParser.getConfigValue("body.null_value_replacement.last_name") == "YES") {
      transformDf = transformDf.na.fill(value = "Unknown", Seq("last_name"))
    }
    //transformDf.show()
    transformDf
  }

  def writeToHiveTable(spark: SparkSession, df: DataFrame, tableName: String): Option[DataFrame] = {
    try{
      logger.info("Writing data into hive table ")
      //df.write.format("csv").save("transformed_csv_emp")
      //write to a hive table
      val tempView = tableName + "_" + "tempView"
      df.createOrReplaceTempView(tempView)
      //drop table
      val dropSqlQuery = "drop table if exists " + tableName
      spark.sql(dropSqlQuery)
      val sqlQuery = " create table " + tableName + " as select * from "  + tempView
      spark.sql(sqlQuery) //creating hive table and populating
      val newDf = spark.sql("select * from " + tempView)
      Some(newDf)
    }catch {
      case e:Exception =>
        logger.error("exception is thrown")
        e.printStackTrace()
        None
    }

  }

}
