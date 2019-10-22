package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 讲师：PK哥   交流群：545916944
  */
object DSApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CatalogApp")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.option("header","true")
        .option("inferSchema","true").csv("data/sales.csv")
    //val ds = df.as[Sales]


    // ROW  DF弱类型
//    df.select("transactionId").show(false)
    println(".....")
    df.printSchema()
//    ds.map(_.transactionId).show(false)

//    spark.sql("xxxx").show(false)

    //ds.map(_.transactionID)


    // RDD vs  DS  vs  DF   ==> DF/DS = RDD + Schema


    spark.stop()
  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
