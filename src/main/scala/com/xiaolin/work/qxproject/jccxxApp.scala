package com.xiaolin.work.qxproject

import com.typesafe.config.ConfigFactory
import com.xiaolin.utils.DateUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 *
 * 获取企薪人员信息
 */
object jccxxApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("SparkSessionApp")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("ID",IntegerType),
      StructField("workername",StringType),
      StructField("type",StringType),
      StructField("worktype_no",StringType),
      StructField("datetime",StringType),
      StructField("isteamleader",StringType),
      StructField("worktype_name",StringType),
      StructField("idcardnumber",StringType),
      StructField("projectcode",StringType)
    ))
//获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.sptn.url")
    val user = config.getString("db.sptn.user")
    val password = config.getString("db.sptn.password")
    val driver = config.getString("db.sptn.driver")
    val ryrdd = spark.sparkContext.textFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_jccxx4\\jkm_jccxx4.txt")
//        .foreach(x=>println(x))
      .map(x => {
        val splits = x.toString().split(",")
        if (splits.length == 9) {
          var str3 = splits(3)
          var str5 = splits(5)
          if(splits(3).isEmpty){
             str3="1000"
          }
          if(splits(5).isEmpty | splits(5)=="false"){
            str5="0"
          }
          var str1 = splits(1)
          if (str1 != null && str1.indexOf(0x00) > -1) {
            str1 = splits(1).replace(0x00.toChar,' ')
          }
          Row(splits(0).toInt, str1, splits(2), str3, DateUtils.changeFormat(splits(4)), str5, splits(6),splits(7),splits(8))
        } else {
          Row(99)
        }
      }).filter(_.get(0)!=99)

    spark.createDataFrame(ryrdd,schema)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("dbtable","jccxx_20200727")
      .option("user",user)
      .option("password",password)
      .save()
//      .coalesce(1).foreach(x=>println(x))
//      .toJavaRDD.rdd.saveAsTextFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_ryxx_error.txt")
//      .write.mode(SaveMode.Overwrite).format("text")
//      .save("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_ryxx_error.txt")



    spark.stop()
  }
}
