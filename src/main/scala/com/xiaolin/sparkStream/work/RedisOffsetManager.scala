package com.xiaolin.sparkStream.work

import com.xiaolin.utils.JedisUtil
import org.apache.kafka.common.TopicPartition

object RedisOffsetManager extends OffsetManager {

  /**
    * 保存偏移量数据
    * @param topic
    * @param groupId
    * @param partition
    * @param offset
    */
  override def storeOffsets(topic: String, groupId: String, partition: Int, offset: Long): Unit = {
      val jedis = JedisUtil.getPoolJedis()
      jedis.hset(topic+"_"+groupId,partition.toString,offset.toString)
      jedis.close()
  }

  /**
    * h获取偏移量数据
    * @param topic
    * @param groupid
    * @return
    */
  override def obtainOffsets(topic: String, groupid: String): Map[TopicPartition, Long] = {
    val jedis = JedisUtil.getPoolJedis()
    val offsetmap = jedis.hgetAll(topic+"_"+groupid)
    import scala.collection.JavaConversions._
    offsetmap.toMap.map(x => {
      new TopicPartition(topic, x._1.toInt) -> x._2.toLong
    })

  }

  def main(args: Array[String]): Unit = {
    //保存offset
    storeOffsets("test","001",1,50)
    //获取数据
    val formoffset = obtainOffsets("test","001")
    formoffset.foreach(x=>{
      println(x._2,x._1)
    })

  }
}
