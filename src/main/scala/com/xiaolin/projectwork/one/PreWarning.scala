package com.xiaolin.projectwork.one

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.util.parsing.json.JSONObject


/**
  *
  */
object PreWarning {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //kafka配置项
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop001:9093,hadoop001:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //获取topic
    val topics = Array("PREWARNING")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        partition.foreach(part=>{
          val x = part.value()
          if (x.contains("INFO") == true || x.contains("WARN") == true || x.contains("ERROR") == true || x.contains("DEBUG") == true || x.contains("FATAL") == true) {

            //转换为json格式
            val json = JSON.parseObject(x.toString)
           // CDHLog(json.getString("time").toString)
          }

          })
      })
    })






    ssc.start()
    ssc.awaitTermination()
  }
case class CDHLog(hostName:String,serviceName:String,lineTimestamp:String,logType:String,logInfo:String)
}
