package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object RDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CatalogApp")
      .getOrCreate()
    import spark.implicits._

    // RDD ==> DF/DS
//    val peopleDF = spark.sparkContext
//      .textFile("data/people.txt")
//      .map(_.split(","))
//      .map(x => Person(x(0), x(1).trim.toInt))
//      .toDF()
//
//    peopleDF.show(false)

//动态创建Schema将非json格式的RDD转换成DataFrame（建议使用
    // step1: Create an RDD
    val peopleRDD = spark.sparkContext.textFile("data/people.txt")

    // step2: The schema is encoded in a string
    val schema = StructType(Array(
      StructField("name",StringType),
      StructField("age",IntegerType)
    ))

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim.toInt))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD,schema)

    peopleDF.show()


    //TODO... 业务逻辑

    spark.stop()
  }

  case class Person(name:String,age:Int)
}
