package main.scala.com.xiaolin.spark01.core01
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  *
  */
object RDDApp1 {

  def main(args: Array[String]): Unit = {

    // step1: SparkConf
    val sparkConf = new SparkConf().setAppName("RDDApp1").setMaster("local[2]")

    // step2: SparkContext
    val sc = new SparkContext(sparkConf)

    // step3: 处理业务逻辑
    //获取数据 对值 进行 年份降序 月份升序 排序
//    val trafficsRdd = sc.textFile("data/traffics").map(x=>{
//      val splits = x.split(" ");
//      (splits(0),(splits(1).toInt,splits(2).toInt,splits(3).toInt))
//    }).groupByKey().
//      mapValues(x=>{
//        x.toList.sortBy(x=>(-x._1,x._2))
//      }).mapValues(x=>{
//      var sb = new StringBuffer
//      for (date<-x){
//        sb.append(date._1).append(" ").append(date._2).append(" ").append(date._3).append("|")
//      }
//      sb.substring(0,sb.length()-1)
//    }).printInfo()
//
//    val input = sc.parallelize(List(
//      List("a",1,3),
//      List("a",2,4),
//      List("b",1,1)
//    ))
//    input.map(x => {
//      val key = x(0).toString
//      val v1 = x(1).toString.toInt
//      val v2 = x(2).toString.toInt
//      (key, (v1,v2))
//    }).reduceByKey((x,y)=>{
//      (x._1 + y._1, x._2+y._2)
//    }).map(x=>List(x._1, x._2._1,x._2._2)).printInfo()

    //广播变量
//    val rdd1 = sc.parallelize(Array(("23","smart"),("9","愤怒的麻雀"))).collectAsMap()
//    val rdd2 = sc.parallelize(Array(("23","郑州"),("9","蜀国"),("14","魔都")))
//
//    val rdd1_bc = sc.broadcast(rdd1)
//
//    rdd2.map(x=>(x._1,x)).mapPartitions(x => {
//      val bc_value = rdd1_bc.value
//
//      for((k,v)<- x if(bc_value.contains(k)))
//        yield (k, bc_value.get(k).getOrElse(""), v._2)
//    }).printInfo()


//    val input = sc.parallelize(List(
//      "1000000,一起看|电视剧|军旅|士兵突击,1,0", // uid,导航,
//      "1000000,一起看|电视剧|军旅|士兵突击,1,1",
//      "1000001,一起看|电视剧|军旅|我的团长我的团,1,1",
//      "1000000,一起看|电视剧|军旅|士兵突击,1,0", // uid,导航,
//      "1000000,一起看|电视剧|军旅|士兵突击,1,1",
//      "1000001,一起看|电视剧|军旅|我的团长我的团,1,1",
//      "1000000,一起看|电视剧|军旅|士兵突击,1,0", // uid,导航,
//      "1000000,一起看|电视剧|军旅|士兵突击,1,1",
//      "1000001,一起看|电视剧|军旅|我的团长我的团,1,1",
//      "1000000,一起看|电视剧|军旅|士兵突击,1,0", // uid,导航,
//      "1000000,一起看|电视剧|军旅|士兵突击,1,1",
//      "1000001,一起看|电视剧|军旅|我的团长我的团,1,1",
//      "1000000,一起看|电视剧|军旅|士兵突击,1,0", // uid,导航,
//      "1000000,一起看|电视剧|军旅|士兵突击,1,1",
//      "1000001,一起看|电视剧|军旅|我的团长我的团,1,1"
//    ))
//
//    /**
//      * 需求：人和“一个东西”的展示量以及点击量
//      * 1）组合key：人和所谓的一个东西
//      *
//      * 1000000 一起看 2 1
//      */
//    val processRDD = input.flatMap(x => {
//      val splits = x.split(",")
//      val id = splits(0).toInt
//      val word = splits(1)
//      val show = splits(2).toInt
//      val clicks = splits(3).toInt
//
//      val words = word.split("\\|")
//      words.map(x => ((id, x), (show, clicks)))
//    })
//
//    /**
//      * 在每个task/partition按照key先进行一个本地的聚合mapSideCombine: Boolean = true
//      * 预聚合之后，在每个task之上对于相同key的数据只有一条
//      *
//      *
//      * 调优 前 vs 后
//      * 是否能按照调优之前和调优之后作业的执行时间来对比?
//      * 时间之外还有其他的：读进来多少数据，shuffle出去多少数据，shuffle读写花费多少时间
//      */
//   // processRDD.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2)).printInfo()
//
//    // 数据全部进行shuffle操作
//    processRDD
//      .groupByKey().mapValues(x=>{
//      val totalShows = x.map(_._1).sum
//      val totalClicks = x.map(_._2).sum
//      (totalShows, totalClicks)
//    }).printInfo()


//分组排序/组内排序
//  * 求每个域名访问量最大的url的Top N
    val TOPN = 2
    val input = sc.textFile("data/site.log")
    val processRDD = input.map(x => {
      val splits = x.split(",")
      val site = splits(0)
      val url = splits(1)
      ((site, url), 1)
    })

    // 分而治之的思路
    //    processRDD.filter(_._1._1 == "www.baidu.com")
    //      .reduceByKey(_+_).sortBy(-_._2)
    //      .take(TOPN).foreach(println)

        //val sites = Array("www.baidu.com","www.google.com","www.twitter.com")
//        for(site <- sites) {
//          processRDD.filter(_._1._1 == site)
//            .reduceByKey(_+_).sortBy(-_._2)
//            .take(TOPN).foreach(println)
//        }

//    val sites = processRDD.map(_._1._1).distinct().collect()  // 数组
//    sites.map(x=>{
//      processRDD.filter(_._1._1 == x).reduceByKey(_+_).sortBy(-_._2) .take(TOPN).foreach(println)
//    })

        processRDD.reduceByKey(_+_)
          .groupBy(_._1._1)
            .mapValues(x => {
              x.toList.sortBy(-_._2)  // toList是一个很大的安全隐患
                .map(x => (x._1._2, x._2)).take(TOPN)
            })
          .printInfo()


    // 11 % 3 = 2  Kafka分区策略
//    val data = sc.makeRDD(List(1,2,3,4,5,6,30,100,300,400,500),3)
//
//    data.mapPartitionsWithIndex((x,partirions)=>
//    {
//      partirions.map(y =>s"$x,是${y}")
//    }).printInfo()
//      .sortByKey()
//      .mapPartitionsWithIndex((index, partition)=>{
//        partition.map(x=>s"分区是$index, 元素是${x._1}")
//      }).printInfo()

















    // step4: 关闭SparkContext
    sc.stop()

  }

}
