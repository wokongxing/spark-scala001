package com.xiaolin.etl

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 日志清洗
  */
object EmpETLV1App extends Logging{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val spark = SparkSession.builder()
      .master("local[5]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .appName("EmpETLV1App")
      .getOrCreate()

    val time = "2019060811"
    val appcalitionnames = spark.sparkContext.getConf.get("spark.app.name")
    val input = s"hdfs://hadoop001:9000/data/offline/emp/raw/$time"
    val output = s"hdfs://hadoop001:9000/data/offline/emp/col/"

    val start = System.currentTimeMillis()
   try{
     val configuration = spark.sparkContext.hadoopConfiguration
     configuration.set("dfs.client.use.datanode.hostname", "true")
     val fileSystem = FileSystem.get(new URI("hdfs://hadoop001:9000"),configuration)
     //获取coalesces
     val coalesces = getCoalescesNum(fileSystem,input+"/*")
     //获取数据数量
     var totals = spark.sparkContext.longAccumulator("totals")
     var errors = spark.sparkContext.longAccumulator("errors")

     var logDF = spark.read.format("text").load(input).coalesce(1)

     logDF = spark.createDataFrame(logDF.rdd.map(x => {
       EmpParser.parseLog(x.getString(0),totals,errors)
     }).filter(_.length !=1),EmpParser.struct)

     logDF.write
       .format("parquet")
       .option("compression","none")
       .partitionBy("day","hour")
       .mode(SaveMode.Overwrite)
       .save(output)
     val end = System.currentTimeMillis()

     log.error(s"$appcalitionnames success,use:${(end-start)/1000}totals:${totals.value},errors:${errors.value}")
   }catch {

     case e=>e.getMessage
       log.error(s"$appcalitionnames:fail"+e.getMessage)
   }finally {
     if (null!=spark){
       spark.close()
     }
   }

  }
  def  getCoalescesNum(flieSystem:FileSystem,path:String,size:Long=128) ={
    var partitions:Long=0
    flieSystem.globStatus(new Path(path)).map(x=>{
      println(x.getPath)
      partitions += x.getLen

    })
    ((partitions/1024/1024)/size).toInt+1
  }
}
