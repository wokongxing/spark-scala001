package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.{Row, SparkSession}
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

    val df = spark.read.textFile("data/emp.txt")
      .map(x => {
        val splits = x.split("")
        val platform = splits(0)
        val platform2 = splits(6)
          (platform,platform2)
      }).toDF("platform","platform2")

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
//
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

//
    spark.sql(sql2).show()


    spark.stop()
  }
}
