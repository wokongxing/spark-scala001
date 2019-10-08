package main.scala.com.xiaolin.spark01.core01

import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.spark.rdd.RDD

/**
  * 讲师：PK哥   交流群：545916944
  */
object TransformationApp {

  def main(args: Array[String]): Unit = {
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName)
    val rdd = sc.parallelize(List(1,2,3,4,5))

    /**
      * map 处理每一条数据
      * mapPartitions 对每个分区进行处理
      *
      * map：100个元素  10个分区 ==> 知识点：要把RDD的数据写入MySQL  Connection
      */
    rdd.map(_*2).printInfo(0)
    rdd.mapPartitions(partition => partition.map(_*2)).printInfo(1)
    rdd.mapPartitionsWithIndex((index, partition) =>{
      partition.map(x => s"分区编号是$index, 元素是$x")
    }).printInfo(1)

    // mapValues 是针对RDD[K,V]的V做处理
    sc.parallelize(List(("ruoze",30),("J哥",18))).mapValues(_ + 1)

    // flatmap = map + flatten
    sc.parallelize(List(List(1,2),List(3,4))).map(x=>x.map(_ * 2))
    sc.parallelize(List(List(1,2),List(3,4))).flatMap(x=>x.map(_*2))

    sc.parallelize(1 to 5).flatMap(1 to _)


    // glom
    sc.parallelize(1 to 30).glom()

    // sample
    sc.parallelize(1 to 30).sample(true, 0.4, 2)

    // filter 留下满足条件的
    sc.parallelize(1 to 30).filter(_ > 20)

    rdd.distinct(4).mapPartitionsWithIndex((index,partition)=>{
      partition.map(x => s"分区是$index,元素是$x")
    })


    // groupKeyKey  RDD[K,V]
    sc.parallelize(List(("a",1),("b",2),("c",3),("a",99))).groupByKey()

    sc.parallelize(List(("a",1),("b",2),("c",3),("a",99))).reduceByKey(_+_)

    /**
      * distinct 去重
      * 不允许使用distinct做去重
      *
      * x => (x,null)
      *
      * 8 => (8,null)
      * 8 => (8,null)
      */
    val b = sc.parallelize(List(3,4,5,6,7,8,8))
    b.map(x => (x,null)).reduceByKey((x,y) => x).map(_._1)

    // groupBy：自定义分组  分组条件就是自定义传进去的
    sc.parallelize(List("a","a","a","b","b","c")).groupBy(x=>x).mapValues(x=>x.size)

    // sortBy
    sc.parallelize(List(("ruoze",30),("J哥",18),("星星",60))).sortBy(_._2)


    sc.parallelize(List(("ruoze",30),("J哥",18),("星星",60))).map(x=>(x._2,x._1)).sortByKey().map(x=>(x._2,x._1))

    val a = sc.parallelize(List(("若泽","北京"),("J哥","上海"),("仓老师","杭州")))
    val c = sc.parallelize(List(("若泽","30"),("J哥","18"),("星星","60")))

    // 第一个：名字  第二个：城市  第三个：年龄  (若泽,(北京,30))
    a.leftOuterJoin(c)
     val value: RDD[(String, (Option[String], Option[String]))] = a.fullOuterJoin(c)

    /**
      * join底层就是使用了cogroup
      * RDD[K,V]
      *
      * 根据key进行关联，返回两边RDD的记录，没关联上的是空
      * join返回值类型  RDD[(K, (Option[V], Option[W]))]
      * cogroup返回值类型  RDD[(K, (Iterable[V], Iterable[W]))]
      */
    val value2: RDD[(String, (Iterable[String], Iterable[String]))] = a.cogroup(c)

    sc.stop()
  }

}
