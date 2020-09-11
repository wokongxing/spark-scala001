package com.xiaolin.es

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.EsSparkSQL

object SparkWriteEs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("estest")
      .master("local[2]")
      //.config("pushdown","true")
      .config("es.nodes","192.168.0.150")
      .config("es.index.auto.create", "true") //创建索引
      .config("es.port","9200")
      .config("es.nodes.wan.only","false")
//      .config("es.write.operation", "upsert")  //update更新方式
//      .config("es.write.operation", "index") //insert更新方式
//      .config("es.mapping.id","app_id") //采用update方式需要指定mapping列
      .config("es.net.http.auth.user", "elastic") //访问es的用户名
      .config("es.net.http.auth.pass", "123456") //访问es的密码
      .getOrCreate()

    val config = ConfigFactory.load()
    val url = config.getString("db.sps.url")
    val user = config.getString("db.sps.user")
    val password = config.getString("db.sps.password")

    val rdd = spark.read.format("jdbc")
      .option("url",url)
      .option("dbtable","sys_openapi_log")
      .option("user",user)
      .option("password",password)
      .load().select("appid","create_time","create_user","data",
      "exception","is_delete","last_modify_time","last_modify_user","method",
      "reason","remark","status","token","version")
//    rdd.show(100)
    EsSparkSQL.saveToEs(rdd,"sys_openapi_log")

    spark.stop()
  }
}
