package main.scala.com.xiaolin.hive

import org.apache.spark.sql.SparkSession

object SparksqlHive {
  def main(args: Array[String]): Unit = {
    val satrt_time = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .enableHiveSupport() //启动hive读取配置文件中的
      .getOrCreate()
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

    val end_time = System.currentTimeMillis()
    println(end_time-satrt_time )
    println(spark.sparkContext.defaultParallelism )
    spark.stop();
  }
}
