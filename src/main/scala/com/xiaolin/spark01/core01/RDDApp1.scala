package main.scala.com.xiaolin.spark01.core01
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 讲师：PK哥   交流群：545916944
  *
  * masterurl： 决定你的作业到底运行在什么模式下
  */
object RDDApp1 {

  def main(args: Array[String]): Unit = {

    // step1: SparkConf
    val sparkConf = new SparkConf().setAppName("RDDApp1").setMaster("local[2]")

    // step2: SparkContext
    val sc = new SparkContext(sparkConf)

    // step3: 处理业务逻辑
    val rdd = sc.parallelize(List(1,2,3,4,5))
    rdd.printInfo()


    // step4: 关闭SparkContext
    sc.stop()

  }

}
