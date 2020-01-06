package com.xiaolin.Test01

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemon {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(List("w", "a", "c", "s", "d", "c"))
    //val rdd2 = sc.textFile("data/a.log")

    val words = rdd.flatMap(_.split(" "))


    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.foreach(println)
    wordCounts.saveAsTextFile("outdata/1")

    sc.stop()
  }
}
