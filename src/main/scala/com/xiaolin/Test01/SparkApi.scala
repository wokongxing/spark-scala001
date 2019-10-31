package com.xiaolin.Test01

import org.apache.spark.{SparkConf, SparkContext}
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object SparkApi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
//    val configuration = new Configuration()
//    configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzopCodec");
//    configuration.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec")
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
//    sparkConf.set("dfs.client.use.datanode.hostname", "true")
//    val sc = new SparkContext(sparkConf)

    //val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,5))
    //rdd.sample(true,0.5,1).printInfo()

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      //.enableHiveSupport() //启动hive读取配置文件中的
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("hdfs://hadoop001:9000/data/Topn.txt").collect()

    val broadcastrdd = sc.broadcast(rdd);
    broadcastrdd.unpersist(true)








    spark.stop()
    sc.stop()
  }

}
