package com.xiaolin.scala04

import java.io.File

import scala.io.Source

//import ImplicitAspect.man2superman
//import ImplicitAspect.file2RichFile
/**
  * 讲师：PK哥
  */
object ImplicitApp {

  def sayHello(msg: String) = println(s"Hello: $msg")
  def sayHello2(implicit msg:String = "若泽") = println(s"Hello: $msg")

  def add(x:Int)(implicit y:Int) = x + y

  def add(x:Int)(implicit y:Int, z:Int) = x + y + z


  implicit class Cal(x:Int) {
    def add(a:Int) = a + x
  }

  implicit class FileEnhance(file:File) {
    def read = Source.fromFile(file.getPath).mkString
  }

  def main(args: Array[String]): Unit = {

    // 2 Int
    println(2.add(4))

    val file = new File("E:\\ruozedata_workspace\\ruozedata-spark\\data\\file.txt")
    println(file.read)

//    implicit val x = 10
//    println(add(3))

    //sayHello("若泽数据")
//    implicit val word = "若泽数据..."  //上下文寻找隐式值（符合参数类型的隐式值）
//    implicit val x = 10
//    implicit val word2 = "ruozedata.com"
//    sayHello2

//    val superman = new Superman("若泽")
//    superman.fly()

//    val man = new Man("J总")
//    man.fly()
//
//    val file = new File("E:\\ruozedata_workspace\\ruozedata-spark\\data\\file.txt")
//    val content = file.read()
//    println(content)
  }



}

class Man(val name:String)

class Superman(val name:String) {
  def fly(): Unit = {
    println(s"$name fly...")
  }
}

class RichFile(val file:File){

  def read() = {
    Source.fromFile(file.getPath).mkString
  }
}