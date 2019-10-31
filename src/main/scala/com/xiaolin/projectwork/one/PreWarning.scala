package com.xiaolin.projectwork.one

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  */
object PreWarning {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))








    ssc.start()
    ssc.awaitTermination()
  }

}
