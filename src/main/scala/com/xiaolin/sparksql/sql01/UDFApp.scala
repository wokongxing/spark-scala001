package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.SparkSession

/**
  * 讲师：PK哥   交流群：545916944
  */
object UDFApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSessionApp")
      .getOrCreate()

    import spark.implicits._
    /**
      * step1： 定义 注册
      * step2： 使用
      */
    val udf = spark.sparkContext.textFile("data/udf.txt")
      .map(_.split("\t"))
      .map(x => FootballTeam(x(0), x(1)))

    udf.toDF().createOrReplaceTempView("teams")

    spark.udf.register("teams_length",(input:String)=>{
      input.split(",").length
    })

    spark.sql("select name,teams,teams_length(teams) from teams").show()

    spark.stop()
  }

  case class FootballTeam(name:String, teams:String)
}
