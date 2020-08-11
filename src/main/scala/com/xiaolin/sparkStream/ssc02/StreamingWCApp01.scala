package com.xiaolin.sparkStream.ssc02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.iq80.leveldb.DB

import scala.collection.mutable.ListBuffer

/**
  *
  */
object StreamingWCApp01 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10));

    //TODO... 填写我们的业务逻辑

    // Input:   socket  Input DStream
    val lines = ssc.socketTextStream("hadoop001",9999)


//    // transformation
    //val result = lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
//
//    // output
    //result.print()

//    result.foreachRDD(rdd => {
//      rdd.foreach(pair => {
//        val connection = MySQLUtils.getConnection()
//        val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
//        connection.createStatement().execute(sql)
//        MySQLUtils.closeConnection(connection)
//      })

//    result.foreachRDD(rdd => {
//      rdd.foreachPartition(partition => {
//        val connection = MySQLUtils.getConnection()
//
//        partition.foreach(pair => {
//          val sql = s"insert into wc(word,cnt) values('${pair._1}', ${pair._2})"
//          connection.createStatement().execute(sql)
//        })
//        MySQLUtils.closeConnection(connection)
//      })
//    })

//    DBs.setupAll()
//    result.foreachRDD(rdd => {
//
//      rdd.foreachPartition(partition => {
//        partition.foreach(pair => {
//
//          DB.autoCommit{ implicit session => {
//            SQL("insert into wc(word,cnt) values(?,?)")
//              .bind(pair._1, pair._2)
//              .update().apply()
//          }
//          }
//        })
//      })
//
//    })

    /**
      * WC这种统计维度来说
      * Redis的使用关键点：如何选择合适的数据类型
      */

//    result.foreachRDD(rdd => {
//      rdd.foreachPartition(partition => {
//        val jedis = RedisUtils.getJedis  // 获取Redis连接
//        partition.foreach(pair => {
//          jedis.hincrBy("ruozedata_redis_wc", pair._1, pair._2)
//        })
//        jedis.close() // free
//      })
//    })

    /**
      * 现在的编程都是基于DStream
      *
      * DStream与RDD互操作咋整？ transform
      *
      *
      * 流处理的时候，有一个数据来源于文本或者是其他的  RDD
      * 另外一个数据是来自Kafka、或者其他的数据源  DStream
      *
      */
    /**
      * 构建黑名单  (xx, true)  (xx, 1)
      */
    val blacks = new ListBuffer[(String,Boolean)]()
    blacks.append(("canglaoshi",true))  // 鉴黄
    val blacksRDD = ssc.sparkContext.parallelize(blacks)  // 把数据转成RDD

    // "日天","也被小卡干了，111111"
    lines.map(x => (x.split(",")(0), x))
      .transform(rdd => {
        rdd.leftOuterJoin(blacksRDD)
          .filter(_._2._2.getOrElse(false) != true)
          .map(x=>x._2._1)
      }).print()

    //lines.count().print()
    //lines.flatMap(_.split(",")).count().print()

//    lines.flatMap(_.split(",")).countByValue().print()
//
//    lines.saveAsTextFiles("")

//    ssc.textFileStream("")


    ssc.start()
    ssc.awaitTermination()


  }
}
