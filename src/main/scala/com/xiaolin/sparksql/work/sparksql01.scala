package com.xiaolin.sparksql.work

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object sparksql01 {

//text多列保存-->将需要保存的数据字段,拼接成一个字段
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext.textFile("data/dept.txt")
    val df = rdd.map(x=>{
      val splits = x.toString().split(",")
      (splits(0),splits(1),splits(2))
    }).toDF("deno","dename","address")
    df.printSchema()
    //获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")

    val jdbcRdd = df.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url",url)
      .option("dbtable","dept")
      .option("user",user)
      .option("password",password)
      .save()

    spark.stop()
  }

}
