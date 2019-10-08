package main.scala.com.xiaolin.spark01.utils

import org.apache.spark.rdd.RDD

object ImplicitAspect {

  private val isprint=0

  implicit class myRdd[T](rdd: RDD[T]){
    def printInfo(flag:Int=0): Unit ={
      if (flag==0){
        rdd.foreach(println)
        println("--------------------分割线-----------------------")
      }
    }
  }

}
