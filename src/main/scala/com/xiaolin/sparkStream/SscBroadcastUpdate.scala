package com.xiaolin.sparkStream

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * 广播变量的变更
 */
object SscBroadcastUpdate {

  def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10));
    val lines = ssc.socketTextStream("hadoop001", 9999)
    val words = lines.flatMap(_.split(" "))


    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    //广播变量的更新
    val sc = ssc.sparkContext
    var broadcastwork = sc.broadcast(Array(("AA",1),("AAA",2)))

    wordCounts.foreachRDD(rdd=>{
      //driver端 变更广播变量
      if (broadcastwork!=null){
        broadcastwork.unpersist()
        broadcastwork = sc.broadcast(rdd.collect())
        println("更新的变量:"+broadcastwork.value.length)
      }
      //workd端
      rdd.foreachPartition(partition=>{
        partition.foreach(part=>{

        })
      })
    })
    println("更新的变量2:"+broadcastwork.value)

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

}
