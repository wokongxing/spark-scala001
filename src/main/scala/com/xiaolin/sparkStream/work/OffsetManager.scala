package com.xiaolin.sparkStream.work

import org.apache.kafka.common.TopicPartition

trait OffsetManager {
  def storeOffsets(topic:String,groupId:String,partition:Int,offset:Long)
  def obtainOffsets(topic:String,groupid:String):Map[TopicPartition,Long]
}
