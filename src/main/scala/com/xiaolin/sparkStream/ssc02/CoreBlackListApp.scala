package com.xiaolin.sparkStream.ssc02


import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._

import scala.collection.mutable.ListBuffer

/**
  * 讲师：PK哥   交流群：545916944
  */
object CoreBlackListApp {

  def main(args: Array[String]): Unit = {

    val sc = ContextUtils.getSparkContext("CoreBlackListApp")

    /**
      * 构建黑名单  (xx, true)  (xx, 1)
      */
    val blacks = new ListBuffer[(String,Boolean)]()
    blacks.append(("苍老师",true))  // 鉴黄
    val blacksRDD = sc.parallelize(blacks)  // 把数据转成RDD

    /**
      * 构建访问日志
      */
    val input = new ListBuffer[(String,String)]
    input.append(("历史第一人","被小卡干了，000000"))
    input.append(("日天","也被小卡干了，111111"))
    input.append(("苍老师","我们敬爱的老师，111111"))
    val inputRDD = sc.parallelize(input)

    //TODO... 想从访问日志中过滤掉“苍老师”的数据
    inputRDD.leftOuterJoin(blacksRDD)
        .filter(_._2._2.getOrElse(false) != true)
        .map(x =>(x._1, x._2._1))
      .printInfo()


    sc.stop()
  }

}
