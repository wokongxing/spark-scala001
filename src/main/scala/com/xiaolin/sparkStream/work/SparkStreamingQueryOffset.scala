package main.scala.com.xiaolin.sparkStream.work

import com.xiaolin.sparkStream.work.MysqlOffsetManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingQueryOffset {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
                  .setAppName(this.getClass.getSimpleName)
                  .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //kafka配置项
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "xiaolindata001:9092,xiaolindata002:9092,xiaolindata003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "PREWARNING_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("pre")
    var stream : InputDStream[ConsumerRecord[String, String]] =null

    //获取偏移量
    val offsets = MysqlOffsetManager.obtainOffsets("pre","PREWARNING_1")

    //从MySql中获取数据进行判断
    if(offsets.isEmpty){
        stream = KafkaUtils.createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams)
            )
    }else {
       stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams, offsets)
        )
    }

    stream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //提交offset
      offsetRanges.foreach(x=>{
        println(x.topic,x.partition,x.fromOffset,x.untilOffset)
        MysqlOffsetManager.storeOffsets(x.topic,"PREWARNING_1",x.partition,x.untilOffset)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
