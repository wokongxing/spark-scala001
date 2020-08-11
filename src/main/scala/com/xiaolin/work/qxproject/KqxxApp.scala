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
      .master("local[6]")
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
    val url = config.getString("pg.oucloud_ods.url")
    val user = config.getString("pg.oucloud_ods.user")
    val password = config.getString("pg.oucloud_ods.password")
    val driver = config.getString("db.sptn.driver")
    val ryrdd = spark.sparkContext.textFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\住建数据处理\\jkm_rykq4_3228\\jkm_rykq4.txt")
      .map(x => {
        val splits = x.toString().split(",")
        if (splits.length == 7) {
          var str3 = ""
          if (!splits(3).isEmpty){
            str3 = DateUtils.changeFormat(splits(3))
          }
          var str2 = splits(2)
          if (str2 != null && str2.indexOf(0x00) > -1) {
            str2 = splits(2).replace(0x00.toChar,' ').trim
          }
//          DateUtils.changeFormat(splits(3))
          Row(splits(0), splits(1), str2, str3 ,splits(4), splits(5),splits(6))
        } else {
          Row(99)
        }
      }).filter(_.get(0)!=99)
    spark.createDataFrame(ryrdd,schema)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("dbtable","jkm_kqxx")
      .option("user",user)
      .option("password",password)
      .save()




    spark.stop()
  }
}
