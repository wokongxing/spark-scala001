package com.xiaolin.sparksql.work

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object sparksql02 {
  def main(args: Array[String]): Unit = {

      val spark = SparkSession
                  .builder()
                  .master("local")
                  .appName(this.getClass.getSimpleName)
                  .getOrCreate()
    //获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val srcTable = config.getString("db.default.srctable")
    val targetTable = config.getString("db.default.targettable")

    val jdbcRdd = spark.read.format("jdbc")
      .option("url",url)
      .option("dbdate",srcTable)
      .option("user",user)
      .option("password",password)
      .load()


  }
}
