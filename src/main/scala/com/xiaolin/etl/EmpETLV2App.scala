package com.xiaolin.etl

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.LongAccumulator

/**
  * 日志清洗
  */
object EmpETLV2App extends Logging{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[5]").appName("EmpETLV2App").getOrCreate()

    val start = System.currentTimeMillis()

    val applicationName = spark.sparkContext.getConf.get("spark.app.name")
    val time = "2019060815"

//    val day = time.substring(0,8)
//    val hour = time.substring(8,10)
    val input = s"hdfs://localhost:8020/ruozedata/offline/emp/raw/$time"
    val output = s"hdfs://localhost:8020/ruozedata/offline/emp/col/"

    try{
      val configuration: Configuration = new Configuration()
      val fileSystem: FileSystem = FileSystem.get(new URI("hdfs://localhost:8020"),configuration)
      var coalesce = getCoalesceSize(fileSystem,input+"/*")

      logError(s"$time.....$coalesce")

      var logDF = spark.read.format("text").load(input).coalesce(coalesce)


      val totals: LongAccumulator = spark.sparkContext.longAccumulator("totals")
      val errors: LongAccumulator = spark.sparkContext.longAccumulator("errors")

      logDF = spark.createDataFrame(logDF.rdd.map(x => {
        EmpParser.parseLog(x.getString(0), totals, errors)
      }).filter(_.length!=1),EmpParser.struct)

      logDF.write.format("parquet").option("compression","none")
        .partitionBy("day","hour")
        .mode(SaveMode.Overwrite).save(output)


      val end = System.currentTimeMillis()

      logError(s"$applicationName success, use:${(end-start)/1000} , totols:${totals.value}, errors:${errors.value}")

    } catch {
      case e => logError(s"$applicationName run exception:" + e.getMessage)
    } finally {
      if(null != spark) {
        spark.close()
      }
    }
  }


  def getCoalesceSize(fileSystem:FileSystem, path:String, size:Long=128) = {
    var partitions = 0l

    fileSystem.globStatus(new Path(path)).map(x => {
      println("xxxx -------" + x.getPath.toString)
      partitions += x.getLen
    })
    (partitions/1024/1024/size).toInt + 1
  }
}
