package com.xiaolin.sparksql.sql02

import org.apache.spark.sql.SparkSession

/**
  * 讲师：PK哥   交流群：545916944
  */
object LogApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("LogApp")
      .getOrCreate()

    // ETL: 一定保留原有的数据   最完整
    var inputDF = spark.read.json("data/data-test.json")


    // ETL==>ODS
    inputDF.coalesce(1).write.format("kudu")
      .option("compression","snappy").save("")

    inputDF.createOrReplaceTempView("log")

    spark.conf.set("spark.sql.shuffle.partitions","400") // --conf
    val areaSQL01 = "select province,city, " +
      "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin_request," +
      "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid_request," +
      "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad_request," +
      "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_cnt," +
      "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_cnt," +
      "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_display_cnt," +
      "sum(case when requestmode=3 and processnode=1 then 1 else 0 end) ad_click_cnt," +
      "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_display_cnt," +
      "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_click_cnt," +
      "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*winprice/1000 else 0 end) ad_consumption," +
      "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*adpayment/1000 else 0 end) ad_cost " +
      "from log group by province,city"
    spark.sql(areaSQL01).show(false)//.createOrReplaceTempView("area_tmp")



    val areaSQL02 = "select province,city, " +
      "origin_request," +
      "valid_request," +
      "ad_request," +
      "bid_cnt," +
      "bid_success_cnt," +
      "bid_success_cnt/bid_cnt bid_success_rate," +
      "ad_display_cnt," +
      "ad_click_cnt," +
      "ad_click_cnt/ad_display_cnt ad_click_rate," +
      "ad_consumption," +
      "ad_cost from area_tmp " +
      "where bid_cnt!=0 and ad_display_cnt!=0"

    Thread.sleep(10000)

    spark.sql(areaSQL02).show(false)

    spark.stop()
  }
}
