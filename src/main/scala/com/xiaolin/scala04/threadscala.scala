package com.xiaolin.scala04


import java.util.concurrent.{ExecutorService, LinkedBlockingQueue}

import org.apache.curator.utils.ThreadUtils

import scala.util.control.NonFatal

/**
 * @program: spark-scala001
 * @description: 实验  LinkedBlockingQueue  队列 堵塞demo
 * @author: linzy
 * @create: 2020-10-21 11:27
 **/
object threadscala {

  private val receivers = new LinkedBlockingQueue[test]

  def main(args: Array[String]): Unit = {

    val threadscala1 = new threadscala()

    Thread.sleep(2000)
    println("____________111______________________________________________")
    threadscala1.registerRpcEndpoint()
    Thread.sleep(10000)

    threadscala1.registerRpcEndpoint2()

    Thread.sleep(100000)

  }



class threadscala {

  def registerRpcEndpoint(){
    receivers.offer(new test("11","11"))
    println("____________2222______________________________________________")
  }
  def registerRpcEndpoint2(){
    receivers.offer(new test(null,null))
    println("____________333333______________________________________________")
  }

  /** Thread pool used for dispatching messages. */
  private val threadpool: ExecutorService = {

    val pool = ThreadUtils.newFixedThreadPool(2, "dispatcher-event-loop")
    for (i <- 0 until 2) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        println("data进入:")
        while (true) {
          try {
            println("data开始:")
              val data = receivers.take()
            if (data == PoisonPill) {
              receivers.offer(PoisonPill)
              println(Thread.currentThread().getId+"--111111111--"+Thread.currentThread().getName)
              return
            }
            Thread.sleep(1000)
            println(Thread.currentThread().getId+"--222222--"+Thread.currentThread().getName)
          } catch {
            case NonFatal(e) => println(e.getMessage, e)
          }
        }
      } catch {
        case _: InterruptedException => // exit
        case t: Throwable =>
          try {
            println("data最后:"+receivers.take())
            threadpool.execute(new MessageLoop)
          } finally {
            throw t
          }
      }
    }
  }
  private val PoisonPill = new test(null,null)
}

  case class test(string: String,string2: String)
}
