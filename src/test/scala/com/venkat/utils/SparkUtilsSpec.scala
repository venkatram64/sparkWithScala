package com.venkat.utils

import com.venkat.MySparkTranslate
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec


class SparkUtilsSpec extends AnyFlatSpec {

  //var spark: SparkSession = _

/*  override def beforeAll()= {
    spark = SparkUtils.createSparkSession()
  }

  override def afterAll() = {
    spark.stop()
  }*/

  def fixture = new {
    val spark = SparkUtils.createSparkSession()
  }

  it should "create the spark session" in {
    val f = fixture
    val spark = f.spark

    assert(spark != None)
  }

  it should " throwing NPE " in {
    try{
      val df: DataFrame = null
      val newDf = MySparkTranslate.replaceNullValues(df)
    }catch {
      case e: NullPointerException =>
        println("NPE thrown")
    }
  }

  it should " cleaning data " in {
    val f = fixture
    val spark = f.spark
    val schema = StructType(Seq(StructField("id", IntegerType),StructField("email", StringType)
      ,StructField("first_name", StringType),StructField("last_name", StringType)))
    val df : DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/test/resources/emp.csv")

    val transformDf = MySparkTranslate.replaceNullValues(df)
    transformDf.show()

    transformDf.select("first_name").show()

    val empRow = transformDf.filter(transformDf("id").equalTo("2"))
      .select("first_name")
      .collectAsList()


    val lastName = empRow.get(0)(0)
    println("emp last name" + lastName)
  }

  it should " assert result cleaning data " in {
    val f = fixture
    val spark = f.spark
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("email", StringType)
      , StructField("first_name", StringType), StructField("last_name", StringType)))
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load("src/test/resources/emp.csv")

    val transformDf = MySparkTranslate.replaceNullValues(df)
    transformDf.show()

    transformDf.select("first_name").show()

    val empRow = transformDf.filter(transformDf("id").equalTo("2"))
      .select("first_name")
      .collectAsList()

    /*assertResult("") {
      empRow.get(0)(0)
    }*/

  }

  it should " assert throwing NPE " in {

    assertThrows[NullPointerException] {
      val df: DataFrame = null
      val newDf = MySparkTranslate.replaceNullValues(df)
    }

  }

}
