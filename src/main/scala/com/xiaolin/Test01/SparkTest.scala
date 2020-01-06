package main.scala.com.xiaolin.Test01

import org.apache.spark.sql.{Row, SparkSession}
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    //测试
    Test01(spark)

    spark.stop()
  }


  def Test01(spark: SparkSession): Unit ={
    val dataFrame = spark.read.format("text").load("data/offline.log")
    dataFrame.rdd.map(x=>{
      val splits = x.getString(0).split("\t")
      val id = splits(0)
      val name = splits(1)
      val time = splits(2)
      val fllow = splits(3)
      Row(id,name,time,fllow)
    }).printInfo()




  }

}
