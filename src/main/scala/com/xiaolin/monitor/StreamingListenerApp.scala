package com.xiaolin.monitor

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
 * @author linzhy
 */
object StreamingListenerApp  extends StreamingListener{
//当前批次数据提交完成 获取的数据信息
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchTime =  batchCompleted.batchInfo.batchTime
    batchCompleted.batchInfo.outputOperationInfos
    batchCompleted.batchInfo.processingStartTime
    batchCompleted.batchInfo.numRecords
    batchCompleted.batchInfo.processingDelay
  }

}
