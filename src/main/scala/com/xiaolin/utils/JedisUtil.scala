package com.xiaolin.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisUtil {
  private val HOST = "114.67.98.145"
  private val PORT =6679 //默认6379
  private val  MAX_ACTIVE=500 //默认值 8 如果赋值为-1，则表示不限制；如果pool已经分配了maxTotal个jedis实例，则此时pool的状态为exhausted(耗尽)
  private val  MAX_IDLE=100 // 默认值 8 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例
  private val MAX_WAIT = 10000 //默认值为-1，表示永不超时。 等待可用连接的最大时间，单位毫秒，如果超过等待时间
                                // 则直接抛出JedisConnectionException

  def getPoolJedis() ={
    val config = new JedisPoolConfig()
    config.setMaxTotal(MAX_ACTIVE)
    config.setMaxIdle(MAX_IDLE)
    config.setMaxWaitMillis(MAX_WAIT)

    val pool = new JedisPool(config,HOST,PORT)
    val jedis = pool.getResource
    jedis
  }










  def main(args: Array[String]): Unit = {
    val jedis = getPoolJedis()
    println(jedis)
    import scala.collection.JavaConverters._
    val list = jedis.smembers("prewarn_set")
    for (x <- list.asScala){
      println(x)
    }
    jedis.close()
  }
}
