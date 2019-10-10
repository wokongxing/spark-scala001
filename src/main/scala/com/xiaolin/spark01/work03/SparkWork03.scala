package com.xiaolin.spark01.work03

import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import scala.collection.mutable.ListBuffer
object SparkWork03 {
//  输入数据：
//  user.txt(id,city,name)  空格分隔
//    1000001 bj douyin
//  1000002 sh yy
//  1000003 bj douyu
//  1000004 sz qqmusic
//  1000005 gz huya
//  traffics.txt(id,year,month,traffic)
//  1000001 2019 9 90
//  1000002 2019 12 20
//  1000003 2019 9 4
//  1000003 2019 7 5
//  1000003 2017 8 6
//  结果：
//  1000001 bj douyin,2019 9 90
//  1000002 sh yy,2019 12 20
//  1000004 sz qqmusic,null null null
//  1000003 bj douyu,2019 7 5|2019 9 4|2017 8 6  年份降序 月份升序
//  1000005 gz huya,null null null

  def main(args: Array[String]): Unit = {
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName);
    //获取user数据
    val userRdd = sc.textFile("data/user").map(x =>{
      val splits = x.split(" ");
      (splits(0),(splits(1),splits(2)))
    })
    userRdd.cache()
    //获取数据 对值 进行 年份降序 月份升序 排序
    val trafficsRdd = sc.textFile("data/traffics").map(x=>{
      val splits = x.split(" ");
      (splits(0),ListBuffer((splits(1).toInt,splits(2).toInt,splits(3).toInt)))
    }).reduceByKey(_++_).mapValues(x=>{
      x.sortBy(x=>(-x._1,x._2))
    }).mapValues(x=>{
      var sb = new StringBuffer
      for (date<-x){
        sb.append(date._1).append(" ").append(date._2).append(" ").append(date._3).append("|")
      }
      sb.substring(0,sb.length()-1)
    })
    trafficsRdd.cache()

    //join
    val resultRdd = userRdd.leftOuterJoin(trafficsRdd).map(x=>{
      val sb = new StringBuffer
      val id = x._1
      val city = x._2._1._1
      val app= x._2._1._2
      val date = x._2._2.getOrElse("null null null")
      sb.append(id).append(" ").append(city).append(" ").append(app).append(" ").append(date)
      sb
    }).printInfo()

    sc.stop();
  }

}
