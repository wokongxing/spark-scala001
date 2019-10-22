package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
  * 讲师：PK哥   交流群：545916944
  */
object LogApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSessionApp")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.textFile("data/access.log")
      .map(x => {
        val splits = x.split("\t")
        val platform = splits(1)
        val traffic = splits(6).toLong
        val province = splits(8)
        val city = splits(9)
        val isp = splits(10)
        (platform, traffic, province, city, isp)
      }).toDF("platform", "traffic", "province", "city", "isp")

      // 如果你想使用SQL来进行处理，那么就是将df注册成一个临时视图
    df.createOrReplaceTempView("log")
//
//    val sql = "select platform, province, city, sum(traffic) as traffics from log group by platform, province, city order by traffics desc"
//    spark.sql(sql).show()

//    import org.apache.spark.sql.functions._
//    df.groupBy("platform", "province", "city")
//        .agg(sum("traffic").as("traffics"))
//          .sort('traffics.desc)
//        .show()

    // platform  组内  province 访问次数最多的TopN



//    val sql =
//      """
//        |
//        |select * from
//        |(
//        |select t.*, row_number() over(partition by platform order by cnt desc) as r
//        |from
//        |(select platform,province,count(1) cnt from log group by platform,province) t
//        |) a where a.r<=3
//        |
//      """.stripMargin

    val sql2 =
      """
        |
        |select a.* from
        |(
        |select t.*,
        |row_number() over(partition by platform order by cnt desc nulls last) as row_,
        |rank() over(partition by platform order by cnt desc nulls last) as rank_,
        |dense_rank() over(partition by platform order by cnt desc nulls last) as dense_
        |from
        |(select platform,province,count(1) cnt from log group by platform,province) t
        |) a
        |
      """.stripMargin


    spark.sql(sql2).show()


    spark.stop()
  }
}
