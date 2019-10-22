package main.scala.com.xiaolin.spark01.utils

import org.apache.spark.{SparkConf, SparkContext}

object ContextUtils {

  def getSparkContext(master:String="local[2]",name:String) ={
    val sparkConf = new SparkConf().setMaster(master).setAppName(name)
     new SparkContext(sparkConf)
  }
  def getSparkContext(name:String) ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(name)

    new SparkContext(sparkConf)
  }
}
