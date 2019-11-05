package com.xiaolin.sparkStream.work

import org.apache.kafka.common.TopicPartition
import scalikejdbc.{DB, DBSession, SQL}
import scalikejdbc.config.DBs
object MysqlOffsetManager extends OffsetManager {
  /**
    * 保存offset
    * @param topic
    * @param groupId
    * @param partition
    * @param offset
    */
  override def storeOffsets(topic: String, groupId: String, partition: Int, offset: Long): Unit ={
    DBs.setupAll()
    DB.autoCommit(implicit session=>{
      SQL("REPLACE INTO kafka_offsets VALUES(?,?,?,?,?)")
        .bind(topic+"_"+groupId+partition.toString,topic,groupId,partition,offset)
        .update().apply()
    })

  }

  /**
    * 获取offsets
    * @param topic
    * @param groupid
    * @return
    */
  override def obtainOffsets(topic: String, groupid: String): Map[TopicPartition, Long] = {
    DBs.setupAll()
    val offsetlist = DB.readOnly(implicit session=>{
      SQL("SELECT topic_partition,end_offset FROM kafka_offsets WHERE topic=? AND groupid=?")
        .bind(topic,groupid)
        .map(x=>{
          (x.int("topic_partition")
          ,x.long("end_offset"))
        }).toList().apply()

    })
    val formoffset = offsetlist.toMap.map(x=>{
      new TopicPartition(topic, x._1.toInt) -> x._2.toLong
    })
    formoffset
  }


  def main(args: Array[String]): Unit = {
    //保存数据
    storeOffsets("kafka","1",1,50);
    //获取偏移量
    val formoffset = obtainOffsets("kafka","1")
    formoffset.foreach(x=>{
      println(x._2,x._1)
    })
  }
}
