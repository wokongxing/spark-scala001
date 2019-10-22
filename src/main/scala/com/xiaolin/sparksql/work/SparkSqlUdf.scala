package com.xiaolin.sparksql.work

import com.xiaolin.utils.Ip2Util
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object SparkSqlUdf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import  spark.implicits._

    val valuerdd = spark.sparkContext.textFile("data/etl_access.log").map(
      x => {
        val splits = x.split("\t")
        //注意 rowrdd (是否缺失字段)
        Row(Some(splits(0)).getOrElse("www"), Some(splits(1)).getOrElse("111.111.111.111"))
      }
    )
    val schema = StructType(Array(
      StructField("domain",StringType),
      StructField("ip",StringType)
    ))
    //自定义函数Udf 注意 导包
    val getcity = udf(Ip2Util.getAaddressByIp(_:String))
    //创建df
    val df = spark.createDataFrame(valuerdd,schema)
    df.select($"domain",getcity($"ip")).show()
    spark.stop()
  }
}
