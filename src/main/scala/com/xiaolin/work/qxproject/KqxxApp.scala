package com.xiaolin.work.qxproject

import com.typesafe.config.ConfigFactory
import com.xiaolin.utils.DateUtils
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 *
 * 获取企薪人员信息
 */
object KqxxApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("SparkSessionApp")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("id",StringType),
      StructField("CellPhone",StringType),
      StructField("WorkerName",StringType),
      StructField("Date",StringType),
      StructField("IDCardNumber",StringType),
      StructField("Direction",StringType),
      StructField("ProjectCode",StringType)

    ))
//获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.sptn.url")
    val user = config.getString("db.sptn.user")
    val password = config.getString("db.sptn.password")
    val driver = config.getString("db.sptn.driver")
    val ryrdd = spark.sparkContext.textFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_kaxx\\jkm_kaxx.txt")
      .map(x => {
        val splits = x.toString().split(",")
        if (splits.length == 7) {
          Row(splits(0), splits(1), splits(2), DateUtils.changeFormat(splits(3)),splits(4), splits(5),splits(6))
        } else {
          Row(99)
        }
      }).filter(_.get(0)!=99)

    spark.createDataFrame(ryrdd,schema)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("driver",driver)
      .option("dbtable","jkm_kqxx_copy1")
      .option("user",user)
      .option("password",password)
      .save()




    spark.stop()
  }
}
