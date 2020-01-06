package com.xiaolin.Test01

import com.xiaolin.utils.Ip2Util
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object EtlJsonData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    //自定义函数Udf
    val province = (ip: String) => {
      var data=""
      if (ip!=null){
        val str= Ip2Util.getAaddressByIp(ip)
        val splits = str.split("\\|")
        if (splits.length==5){
            data = splits(2)
        }
      }
      Some(data).getOrElse(null)
    }
    //自定义函数Udf
    val city = (ip: String) => {
      var data=""
      if (ip!=null){
        val str= Ip2Util.getAaddressByIp(ip)
        val splits = str.split("\\|")
        if (splits.length==5){
          data = splits(3)
        }
      }
      Some(data).getOrElse(null)
    }

    val getprovince = udf(province)
    val getcity = udf(city)
    // ETL: 一定保留原有的数据   最完整
    val intput = "hdfs://hadoop001:9000/data/spark/data-test.json"
    val intput2 = "data/data-test.json"
    val jsondf = spark.read.option("inferSchema","true").format("json").load(intput2)
    jsondf.select("adorderid","adplatformproviderid","ip","requestmode","processnode","iseffective","isbilling","isbid","iswin")
        .withColumn("province",getprovince(jsondf("ip")))
        .withColumn("city",getcity(jsondf("ip")))
        .show(20)
        //.write.mode(SaveMode.Overwrite)
        //.option("compression","snappy")
        //.format("parquet")
        //.save("hdfs://hadoop001:9000/outdata/spark/work01")
    //compression压缩  orc parquet 列式存储
    spark.stop();
  }

}
