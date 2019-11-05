package com.xiaolin.projectwork.one

import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import com.xiaolin.projectwork.one.PreWarning.BroadcastPrewarn.broadcastPrewarnlist
import com.xiaolin.utils.JedisUtil
import com.xiaolin.utils.JedisUtil.getPoolJedis
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.collection.JavaConversions._
/**
  *
  */
object PreWarning {
  private val dbName = "prwarndb"
  private var sql = "";
  @volatile private var broadcastPrewarnlist:Broadcast[java.util.Set[String]]=null
  private var sqlother =""
  private var value =""
  private var host_service_logtype =""

  private val schema = StructType(Array(
    StructField("hostname",StringType),
    StructField("servicename",StringType),
    StructField("time",StringType),
    StructField("logtype",StringType),
    StructField("loginfo",StringType)
  ))
   private var lastUpdatedAt = Calendar.getInstance.getTime //上次time
  def main(args: Array[String]): Unit = {
    //获取influx数据库连接
    val influxDB = InfluxDBFactory.connect("http://" + InfluxDBUtils.getInfluxIP + ":" + InfluxDBUtils.getInfluxPORT(true), "admin", "admin")
    val rp = InfluxDBUtils.defaultRetentionPolicy(influxDB.version)

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //kafka配置项
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "xiaolindata001:9092,xiaolindata002:9092,xiaolindata003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "PREWARNING_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //获取offset
    //    val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
    //      new TopicPartition(resultSet.string("topic"), resultSet.int("partition")) -> resultSet.long("offset")
    //    }.toMap
    //获取topic
    val topics = Array("pre")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.foreachRDD(rdd=>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (rdd.count()>0){
        //获取数据 过滤数据
        val temprdd = rdd.map(part => {
          val x = part.value()
          if (x.contains("INFO") == true || x.contains("WARN") == true || x.contains("ERROR") == true || x.contains("DEBUG") == true || x.contains("FATAL") == true) {
            //转换为json格式
            val json = JSON.parseObject(x.toString)
            Row(json.getString("hostname"),
              json.getString("servicename"),
              json.getString("time"),
              json.getString("logtype"),
              json.getString("loginfo"))
          } else {
            Row(999)
          }
        }).filter(_ != 999)

        //转成 df
        val dataFrame = spark.createDataFrame(temprdd,schema)
        dataFrame.show(false)

        //创建视图
        dataFrame.createOrReplaceTempView("prewarninglog")
        //获取高危词 redis
        //broadcastPrewarnlist = BroadcastPrewarn.updateAndGet(spark.sparkContext)
        //当前time
        val currentDate = Calendar.getInstance.getTime
        //time差值
        val diff = currentDate.getTime - lastUpdatedAt.getTime
        if(broadcastPrewarnlist==null|| diff >= 60000){
          if (broadcastPrewarnlist!=null){
            broadcastPrewarnlist.unpersist()
          }
          //再次更新上次time
          lastUpdatedAt= new Date(System.currentTimeMillis)
          val jedis = getPoolJedis()
          val set = jedis.smembers("prewarn_set")
          broadcastPrewarnlist = spark.sparkContext.broadcast(set)
          jedis.close()
        }


        if (broadcastPrewarnlist.value.size()>0){
          sqlother=""
          for (x:String<-broadcastPrewarnlist.value){

            sqlother = sqlother + " logInfo like '%" + x + "%' or"
          }
          sqlother = sqlother.substring(0, sqlother.length() - 2);
          sql = "SELECT hostName,serviceName,logType,COUNT(logType) " +
            "FROM prewarninglog GROUP BY hostName,serviceName,logType" +
            " union all " + "SELECT t.hostName,t.serviceName,t.logType,COUNT(t.logType) FROM "+
            "(SELECT hostName,serviceName,'alert' as logType FROM prewarninglog where " +
            sqlother + ") t " + " GROUP BY t.hostName,t.serviceName,t.logType"

        }else {
          sql = "SELECT hostName,serviceName,logType,COUNT(logType) FROM prewarninglog GROUP BY hostName,serviceName,logType"
        }
        //获取计算结果
        val list = spark.sql(sql).collectAsList()

        //保存数据到influx
        value = ""
        //循环处理
        for (rowlog <- list) {
          host_service_logtype = rowlog.get(0) + "_" + rowlog.get(1) + "_" + rowlog.get(2)
          value = value + "prewarning,host_service_logtype=" + host_service_logtype + " count=" + String.valueOf(rowlog.getLong(3)) + "\n"
        }
        //存储至influxdb
        if (value.length > 0) {
          value = value.substring(0, value.length) //去除最后一个字符“,”
          //打印
          System.out.println(value)
          //保存
          influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
        }
      }else{
        println(s"暂无数据")
      }


      //提交offset
      offsetRanges.foreach(x=>{
        println(x.topic,x.partition,x.fromOffset,x.untilOffset)
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })






    ssc.start()
    ssc.awaitTermination()
  }

  object  BroadcastPrewarn{


    private var broadcastPrewarnlist: Broadcast[java.util.Set[String]] = null
    def updateAndGet(sc:SparkContext) ={

      broadcastPrewarnlist
      }
  }
}
