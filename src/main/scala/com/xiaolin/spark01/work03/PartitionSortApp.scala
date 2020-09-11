package com.xiaolin.spark01.work03

import main.scala.com.xiaolin.spark01.utils.ContextUtils
import org.apache.spark.Partitioner
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
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

    val reducerdd = rdd.reduceByKey(urlpartitioner, _ + _)
    val reducerdd2 = rdd.aggregateByKey(0,2)

    reducerdd.mapPartitions(_.toList.sortBy(-_._2).map(x=>{
        (x._1._1,(x._1._2,x._2))
      }).take(TOPN).iterator
    ).groupByKey().printInfo()

    sc.stop()

  }

  //自定义分区器
   class UrlPartitioner(urls:Array[String] ) extends Partitioner{

    //分区规则
     val rules=new mutable.HashMap[String,Int]()
     var index=0
     for (ulr<-urls){
       rules.put(ulr,index)
       index+=1
     }

      //获取分区数
     override def numPartitions: Int = urls.length
    //分区
     override def getPartition(key: Any): Int = {

       rules(key.asInstanceOf[(String,String)]._1)
     }
   }

}
