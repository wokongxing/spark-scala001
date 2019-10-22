package com.xiaolin.scala04

/**
  * Scala中的泛型：类型的约束
  * 讲师：PK哥
  */
object GenericApp {

  def main(args: Array[String]): Unit = {
//    val mm1 = new MM[Int,CupEnum,Int](90, CupEnum.A, 175)
//    val mm2 = new MM[Int,CupEnum,Int](60, CupEnum.F, 170)
//    val mm3 = new MM[Int,CupEnum,Int](80, CupEnum.B, 165)
//
//    println(mm1)
//    println(mm2)
//    println(mm3)


//    test3[UserA](new UserA)
//    test3[Person](new Person)
//    test3[Child](new Child)

    // 泛型类型是不可变
    // UserA  ==> Child  协变  增强/补充
//    val test:Test[UserA] = new Test[Child]
//    println(test)

    // UserA ==> Person  逆变 减少
    val test:Test[UserA] = new Test[Person]
    println(test)

    val ints = List(10,20,30,40)
//    ints.reduceLeft[UserA]()  // 下界
  }


  def test[T](t:T): Unit ={
    println(t)
  }

  // <: 泛型的上限，传递参数时，应该是当前类或者子类
  def test2[T <: UserA](t:T): Unit ={
    println(t)
  }

  // >: 泛型的下限，传递参数时，应该是当前类或者父类
  def test3[T >: UserA](t:T): Unit ={
    println(t)
  }

}

class Person
class UserA extends Person
class Child extends UserA
class Test[-UserA]

abstract class Msg[T](content:T)
class WebChatMsg[String](content:String) extends Msg(content)
class DigitMsg[Int](content:Int) extends Msg(content)

class MM[A, B, C](val faceValue:A, val cup:B, val height:C) {
  override def toString: String = faceValue + "\t" + cup + "\t" + height
}

//Scala中枚举的使用
object CupEnum extends Enumeration {
  type CupEnum = Value
  val A,B,C,D,E,F,G = Value
}