package com.xiaolin.scala04

/**
  * 讲师：PK哥
  */
object UpperLowerBoundsApp {

//  implicit def user2OrderedUser(user:User) = new Ordered[User]{
//    override def compare(that: User): Int = user.age - that.age
//  }


  def main(args: Array[String]): Unit = {

//    val maxInt = new MaxInt(10, 13)
//    println(maxInt.compare)
//
//    val maxLong = new MaxLong(10, 13)
//    println(maxLong.compare)

//    val maxValue = new MaxValue(Integer.valueOf(10), Integer.valueOf(13))
//    val maxValue = new MaxValue2(10,13)
//    println(maxValue.compare)

    val user1 = new User("若泽",30)
    val user2 = new User("J总",18)

    implicit val cmptor = new Ordering[User] {
      override def compare(x: User, y: User): Int = x.age - y.age
    }

    println(new MaxValue3(user1, user2).compare)
  }
}

class MaxInt(x:Int, y:Int) {
  def compare = if(x > y) x else y
}

class MaxLong(x:Long, y:Long) {
  def compare = if(x > y) x else y
}

// TODO... 穷举死人哦...

class MaxValue[T <: Ordered[T]](x:T, y:T) {
  def compare = if(x.compareTo(y)>0) x else y
}

// 视图界定：底层隐式转换了
class MaxValue2[T <% Ordered[T]](x:T, y:T) {
  def compare = if(x.compareTo(y)>0) x else y
}

// 上下文界定
class MaxValue3[T : Ordering](x:T, y:T)(implicit  cmptor: Ordering[T]){
  def compare = if(cmptor.compare(x,y)>0) x else y
}

class User(val name:String, val age:Int)  { //extends Ordered[User]
  //override def compare(that: User): Int = this.age - that.age
  override def toString: String = name + " \t " + age
}

