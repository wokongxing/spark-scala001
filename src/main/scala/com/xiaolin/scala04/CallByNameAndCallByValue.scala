package com.xiaolin.scala04

/**
 * @program: spark-scala001
 * @description: call by name 参数传入函数时,会先自己计算
 * @author: linzy
 * @create: 2020-09-18 16:22
 **/
object CallByNameAndCallByValue {

    var t=1

  def main(args: Array[String]) =
  {
//    callbyvalue(add())
//    println("---------------------------------")
    callbyname(add())

  }

  def callbyname(t: => Int)={
    for(i <- 1 to 10){
      println(t)
    }

  }
  def callbyvalue(t:Int)={
    for(i <- 1 to 10){
      println(t)
    }

  }

  def add()  ={
    t+=1
    t
  }

}
