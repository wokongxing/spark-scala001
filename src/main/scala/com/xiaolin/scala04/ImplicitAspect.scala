package com.xiaolin.scala04

import java.io.File

/**
  * 讲师：PK哥
  */
object ImplicitAspect {

  // 定义隐式转换函数  2 to  Scala会自动帮我们完成转换
  // 公式： implicit def x2y(普通的X)：牛逼的Y = new 牛逼的Y(...)
  implicit def man2superman(man:Man):Superman = new Superman(man.name)


  implicit def file2RichFile(file:File):RichFile = new RichFile(file)

}
