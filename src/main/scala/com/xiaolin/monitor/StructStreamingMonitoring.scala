package com.xiaolin.monitor

import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * @author linzhy
 */
object StructStreamingMonitoring extends StreamingQueryListener {
  //结构化流启动的时候异步回调
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    event.
  }

//询过程中的状态发生更新时候的异步回调
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    event.progress.sink.
  }
//查询结束实时的异步回调
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {

  }
}
