package com.xiaolin.etl

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 日志清洗
  */
object EmpETLShellApp extends Logging{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val time = spark.sparkContext.getConf.get("spark.time", "");
    if (time == "") {
      System.exit(0)
    }
    var input = spark.sparkContext.getConf.get("spark.input", "hdfs://hadoop001:9000/data/offline/emp/raw/")
    input = input+time
    var output = spark.sparkContext.getConf.get("spark.output", "hdfs://hadoop001:9000/data/offline/emp/col/")
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val appcalitionnames = spark.sparkContext.getConf.get("spark.app.name")

    val start = System.currentTimeMillis()
    try {
      val configuration = spark.sparkContext.hadoopConfiguration
      //      configuration.set("dfs.client.use.datanode.hostname", "true")
      val fileSystem = FileSystem.get(configuration)
      //获取coalesces
      val coalesces = getCoalescesNum(fileSystem, input + "/*")
      //获取数据数量
      var totals = spark.sparkContext.longAccumulator("totals")
      var errors = spark.sparkContext.longAccumulator("errors")

      var logDF = spark.read.format("text").load(input).coalesce(coalesces)
      logDF.repartition()
      logDF = spark.createDataFrame(logDF.rdd.map(x => {
        EmpParser.parseLog(x.getString(0), totals, errors)
      }).filter(_.length != 1), EmpParser.struct)

      logDF.sortWithinPartitions().write
        .format("parquet")
        .option("compression", "none")
        .partitionBy("day", "hour")
        .mode(SaveMode.Overwrite)
        .save(output)
      val end = System.currentTimeMillis()

      log.error(s"$appcalitionnames success,use:${(end - start) / 1000}totals:${totals.value},errors:${errors.value}")
    } catch {

      case e:Exception =>
        log.error(s"$appcalitionnames:fail" + e.getMessage)
    } finally {
      if (null != spark) {
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
