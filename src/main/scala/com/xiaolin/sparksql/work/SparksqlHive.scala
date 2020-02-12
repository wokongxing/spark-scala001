package com.xiaolin.sparksql.work

import org.apache.spark.sql.{SaveMode, SparkSession}
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
object SparksqlHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .enableHiveSupport() //启动hive读取配置文件中的
      .getOrCreate()
    import spark.implicits._
    val sql = "show tables"
    val sql2 ="select * from employee"
//    val df = spark.read.format("text").load("data/emp.txt")
//        .map(x=>{
//          val splits = x.toString().split(",")
//          (splits(0).substring(1,splits(0).length),splits(1),splits(2),splits(3),splits(4),splits(5),splits(6).replaceAll("]",""))
//        }).
//    toDF("emno","name","duty","other","salary","jrsj","deno")
//    df.printSchema()
//    df.write.mode(SaveMode.Overwrite).saveAsTable("emp")

    spark.sql(sql2).show(10)
    spark.stop();
  }
}
