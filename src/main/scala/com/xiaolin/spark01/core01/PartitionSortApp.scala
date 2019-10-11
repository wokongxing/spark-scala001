package com.xiaolin.spark01.core01

import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.spark.Partitioner

import scala.collection.mutable
object PartitionSortApp {

//使用自定义分区器实现分组TopN
  def main(args: Array[String]): Unit = {

    val TOPN = 2
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName)
    val rdd = sc.textFile("data/site.log").map(x => {
      val splits = x.split(",")
      ((splits(0), splits(1)), 1)
    })
    //获取
    val sites = rdd.map(_._1._1).distinct().collect()

    val urlpartitioner = new UrlPartitioner(sites)

    rdd.reduceByKey(urlpartitioner, _ + _).mapPartitions(ps => {
      ps
    })


  }


   class UrlPartitioner(urls:Array[String] ) extends Partitioner{

     val rules=new mutable.HashMap[String,Int]()
     var index=0
     for (ulr<-urls){
       rules.put(ulr,index)
       index+=1
     }
      //获取分区数
     override def numPartitions: Int = urls.length

     override def getPartition(key: Any): Int = {

       rules(key.asInstanceOf[(String,String)]._1)
     }
   }

}
