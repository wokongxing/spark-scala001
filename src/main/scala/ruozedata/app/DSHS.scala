package ruozedata.app

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import ruozedata.util.{GJSONParserUtil, PhoenixUtil}


/**
  * mysql 实时同步数据至 hbase
  *
  *
  * DSHS v0.1
  *1.调研MySQL-->HBase的实时数据线
  *2.使用Direct方式从Kafka0.10 cluster读取json数据,且打印
  *3.将json格式解析成Phoenix sql
  *
  *
  * DSHS v0.2
  * 1.整条线及单个组件的压力测试，主要性能瓶颈在MySQL写到binlog
  * 2.maxwell写到kafka的零丢失
  * 3.维护offset，使用commitAsync API来将消费的offset保证至kafka。
  *
  *
  * 写HBase  因为使用rdd.asInstanceOf[HasOffsetRanges].offsetRanges，
  * 所以在createDirectStream之后，要紧跟着foreachRDD，
  * 中间不能有window,sortByKey等函数转换，否则要抛错，
  * 无法转换类型错误MapPartitionsRDD cannot be cast to HasOffsetRanges
  *
  *
  * DSHS v0.3
  * 1.添加批量提交    https://phoenix.apache.org/tuning_guide.html
  * 2.添加链接池pool  https://issues.apache.org/jira/browse/PHOENIX-2388(因为序列化和闭包问题，官方不推荐，暂且不用)
  *
  * DSHS v0.4
  * 1.由于科学计数法，使用Google json解析json转phoenix sql
  *
  */

object DSHS {



  def main(args: Array[String]): Unit = {
    //定义变量
    val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")
    println("启动时间：" + timeFormat.format(new Date()))

    val slide_interval = Seconds(3)
    //kakfa地址
    val bootstrap_servers = "ruozedata001:9092,ruozedata002:9092,ruozedata003:9092"

    try {

      //1. Create context with 2 second batch interval

      val ss = SparkSession.builder()
        .appName("DSHS-0.4")
        .master("local[2]")
        .getOrCreate()
      val sc = ss.sparkContext
      val scc = new StreamingContext(sc, slide_interval)

      //2.设置kafka的map参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrap_servers
        , "key.deserializer" -> classOf[StringDeserializer]
        , "value.deserializer" -> classOf[StringDeserializer]
        , "group.id" -> "ruozedataf17"
        , "auto.offset.reset" -> "latest"
        , "enable.auto.commit" -> (false: java.lang.Boolean)
        , "max.partition.fetch.bytes" -> (2621440: java.lang.Integer) //default: 1048576
        , "request.timeout.ms" -> (150000: java.lang.Integer) //default: 60000
        , "session.timeout.ms" -> (120000: java.lang.Integer) //default: 30000
        , "heartbeat.interval.ms" -> (40000: java.lang.Integer)
      )

      //3.创建要从kafka去读取的topic的集合对象
      val topics = Array("DSHS")
      //4.输入流
      val directKafkaStream = KafkaUtils.createDirectStream[String, String](
        scc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )




      directKafkaStream.foreachRDD(
        rdd => {

          println("------------rdd begin process-------------------------")

          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //取出offsetRanges信息

          var sql = ""
          var sqlstr = ""

          rdd.foreachPartition(rows => {

            val phoenixConn = new PhoenixUtil()
            //批量提交 3000提交一次
            var batchSize = 0
            val commitSize = 100 // number of rows you want to commit per batch.
            //获取元数据
            rows.foreach { row =>
              //解析成phoenix sql
              //sql = JSONParserUtils.parseJSONToPhoenixSQL(row.value().trim)  //v0.3
              sql = GJSONParserUtil.parseJSONToPhoenixSQL(row.value().trim) //v0.4 使用google json解析，解决小数转科学计数法

              if (sql != "error") {
                println("currentTime : " + timeFormat.format(new Date()) + " : " + sql)

                //保存至HBase
                phoenixConn.saveToHBase(sql)

                batchSize = batchSize + 1
                if (batchSize % commitSize == 0) {
                  //每commitSize提交一次
                  phoenixConn.conn.commit()
                }
              }
            }
            println("------------last commit and close connection-------------------------")
            // commit the last batch of records
            phoenixConn.closeCon()

          })



          directKafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) //rdd处理完 异步提交offset to kafka ，保证数据零丢失
          println("------------rdd end process,and offset also commit-------------------------")
        }
      )

      scc.start()
      scc.awaitTermination()
      scc.stop()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }
}
