package com.xiaolin.Test01

import org.apache.spark.{SparkConf, SparkContext}
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._

object SparkApi {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName))
    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,5))
    //rdd.sample(true,0.5,1).printInfo()














    sc.stop()
  }

}
