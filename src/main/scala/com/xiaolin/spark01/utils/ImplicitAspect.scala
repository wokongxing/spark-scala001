package main.scala.com.xiaolin.spark01.utils

import org.apache.spark.rdd.RDD

object ImplicitAspect {

  implicit class pringRdd[T](rdd: RDD[T]){
     def printInfo(flag:Int=0): Unit ={
      if (flag==0){
        println("--------------------分割线-----------------------")
        rdd.foreach(println)
      }
    }
  }

}
