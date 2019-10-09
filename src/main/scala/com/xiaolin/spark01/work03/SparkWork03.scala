package com.xiaolin.spark01.work03

import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
object SparkWork03 {

  def main(args: Array[String]): Unit = {
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName);

//    val userRdd = sc.textFile("data/user").map(x =>{
//      val splits = x.split(" ");
//      (splits(0),(splits(1),splits(2)))
//    }).printInfo(1)

    val trafficsRdd = sc.textFile("data/traffics").map(x=>{
      val splits = x.split(" ");
      (splits(0),(splits(1).toInt,splits(2).toInt,splits(3).toInt))
    }).reduceByKey((_)).printInfo()


    sc.stop();
  }

}
