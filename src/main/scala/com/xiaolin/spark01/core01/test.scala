package main.scala.com.xiaolin.spark01.core01

object test {
  def main(args: Array[String]): Unit = {
    val stringToString = System.getenv()
    println(System.getenv("JAVA_HOME"))
    println(stringToString.get("JAVA_HOME"))
    println(System.getProperties)
  }
}
