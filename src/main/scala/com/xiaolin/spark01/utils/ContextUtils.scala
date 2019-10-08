package main.scala.com.xiaolin.spark01.utils

import org.apache.spark.{SparkConf, SparkContext}

object ContextUtils {

  def getSparkContext(name:String) ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(name)

    // step2: SparkContext
     new SparkContext(sparkConf)
  }
}
