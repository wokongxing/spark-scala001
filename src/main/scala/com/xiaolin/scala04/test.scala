package com.xiaolin.scala04

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

/**
 * 科理化 偏函数
 */
object test {
  def main(args: Array[String]): Unit = {
//    //科理化
//    val file = makeFile(".scala")
//    println(file("cat"))
//    println(file("dog.scala"))
//
//    //偏应用函数
//    def add(x:Int,y:Int,z:Int) = x+y+z
//    def addX = add(1,_:Int,_:Int) // x 已知
//    addX(2,3)
//
//    //偏函数
//    val pf:PartialFunction[Int,String] = {
//        case 1=>"One"
//         case 2=>"Two"
//        case 3=>"Three"
//        case _=>"Other"
//      }
//    println(pf(1))
//
    val test1 = new test()
  }
    //柯里化
  def makeFile(suffix: String)=(fileName: String)=>  {
    if (fileName.endsWith(suffix))
      fileName
    else
      fileName + suffix
  }

  private class test{
    val mk = new test2();
  }
  private class test2{
    start()
    def init: Unit ={
      println("初始化..........")
    }
    def start(): Unit ={
      println("触发了")
    }
  }
}
