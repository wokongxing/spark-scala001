package com.xiaolin.scala04

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-10-15 14:02
 **/
object collectApp {

  def main(args: Array[String]): Unit = {
//    val intToInt:Array[(Int,Int)] = new Array[(Int,Int)](10)
    val intToInt:Array[(Int,Int)] = Array.ofDim[(Int,Int)](3)
    val intToInt2:Array[(Int,Int)] = Array.ofDim[(Int,Int)](3)
    intToInt(0)=(1,2)
    intToInt(1)=(2,33)
    intToInt(2)=(31,44)

    intToInt2(0)=(11,2)
    intToInt2(1)=(21,33)
    intToInt2(2)=(31,44)

    println(intToInt.union(intToInt2).distinct.toList)
    for (i <- 0 until intToInt.length) {
      println(intToInt(i))
      }

    }


}
