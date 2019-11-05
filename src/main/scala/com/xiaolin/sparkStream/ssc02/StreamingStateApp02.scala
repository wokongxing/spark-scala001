package com.xiaolin.sparkStream.ssc02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingStateApp02 {

  val checkpointDirectory = "."

  def main(args: Array[String]): Unit = {
    // 当作业挂了时，从checkpoint中去获取StreamingContext
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10));
    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    val lines = ssc.socketTextStream("ruozedata001",9999) // create DStreams
    val result = lines.flatMap(_.split(","))
      .map((_,1))
      .updateStateByKey(updateFunction)
    result.print()

    ssc
  }

  /**
    *
    * 1)  a a a d d
    * 2)  b b b c c a
    *
    * @param newValues  当前批次的值
    *        key对应的新值  可能有多个 所以是一个Seq
    * @param preValues  以前批次的累加值
    *        key已经存在的值  有可能没有 有可能有  所以定义成Option
    * @return
    */
  def updateFunction(newValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val curr = newValues.sum // 当前
    val pre = preValues.getOrElse(0)
    Some(curr + pre)
  }
}
