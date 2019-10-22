package com.xiaolin.Test01

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object StatJsonData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .appName(this.getClass.getSimpleName)
      ////      .master("local")
      .getOrCreate()
    val jsondf = spark.read.option("inferSchema","true").format("json").load("hdfs://hadoop001:9000/outdata/spark/work01/*.json")
    //jsondf.show()
    jsondf.printSchema()
    jsondf.createOrReplaceTempView("SYS_AD")
    //需要设置分区 默认200 过少
    val sql =
      """
        |select AD.* ,
        |ROUND(AD.adbidseccesscounts/AD.adbidcounts * 100, 2) bidrate,
        |ROUND(AD.adclickcounts/AD.addispalycounts * 100, 2) clickrate
        |from
        |(
        |select
        |province,
        |city,
        |sum (case when requestmode=1 and processnode>=1 then 1 else 0 end) requestmodecounts,
        |sum (case when requestmode=1 and processnode>=2 then 1 else 0 end) processnodecounts,
        |sum (case when requestmode=1 and processnode=3 then 1 else 0 end) adrequestcounts,
        |sum (case when adplatformproviderid>=100000 and iseffective=1
        |     and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) adbidcounts,
        |sum (case when adplatformproviderid>=100000 and iseffective=1
        |     and isbilling=1 and iswin=1 then 1 else 0 end) adbidseccesscounts,
        |sum (case when requestmode=2 and iseffective=1 then 1 else 0 end) addispalycounts,
        |sum (case when requestmode=3 and iseffective=1 then 1 else 0 end) adclickcounts,
        |sum (case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) mediadispalycounts,
        |sum (case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) mediaclickcounts,
        |sum (case when adplatformproviderid>=100000 and iseffective=1
        |     and isbilling=1 and iswin=1 and adorderid>200000 then 1 else 0 end) adconsumecounts,
        |sum (case when adplatformproviderid>=100000 and iseffective=1
        |     and isbilling=1 and iswin=1 and adorderid>200000 then 1 else 0 end) adcostcounts
        |from SYS_AD  GROUP BY province,city) AD
        |
      """.stripMargin
    val resultdf = spark.sql(sql)
    //写入sql
    //获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")
    val targetTable = config.getString("db.default.targettable")

    val jdbcRdd = resultdf.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url",url)
      .option("dbtable",targetTable)
      .option("user",user)
      .option("password",password)
      .save()

    spark.stop()
  }
}
