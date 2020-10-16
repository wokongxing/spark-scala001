package com.xiaolin.spark01.core01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
 * @program: spark-scala001
 * @description:
 *  topN   分组取TopN  （二次排序）
 *  条件:
 *    1. 同月份中 同一天中取 最高温度值 2019-6-1	39
 *    2. 同月份中 温度最高的2天
 * @author: linzy
 * @create: 2020-10-15 11:20
 **/
object combineBykeyApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("TOPN")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("error")

    val file: RDD[String] = sc.textFile("data/tqdata")
    //2019-6-1	39
    val data2 = file.map(line => line.split("\t")).map(arr => {
      val arrs: Array[String] = arr(0).split("-")
      //(year,month,day,wd)
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })

//    val data3: RDD[Row] = data2.map(x => Row(x._1, x._2, x._3, x._4))
//
//    val schema = StructType(Array(
//      StructField("year",IntegerType),
//      StructField("month",IntegerType),
//      StructField("day",IntegerType),
//      StructField("tp",IntegerType)
//    ))
//    val dataFrame = spark.createDataFrame(data3, schema)
//
//    dataFrame.createOrReplaceTempView("data_test")
//
//    val sql =
//      """
//        |select year,month,concat_ws(",",collect_set(concat("-",day,tp))) from (
//        |select *,row_number() over( partition by year,month order by tp desc) top2 from (
//        | select *,row_number() over( partition by year,month,day order by tp desc) top1
//        |   from data_test
//        | ) a where a.top1=1
//        | ) a  where a.top2 <=2 group by year,month
//        |""".stripMargin
//
//
//
//    spark.sql(sql).show(false)

    implicit val topNOrdering = new Ordering[(Int, Int)] { //降序
      override def compare(x: (Int, Int), y: (Int, Int)) = {
        y._2.compareTo(x._2)
      }
    }

    //分组    取topN  (排序)
    //第五代
    //分布式计算的核心思想：调优天下无敌：combineByKey
    //分布式是并行的，离线批量计算有个特征就是后续步骤(stage)依赖其一步骤(stage)
    //如果前一步骤(stage)能够加上正确的combineByKey
    //我们自定的combineByKey的函数，是尽量压缩内存中的数据

    val kv: RDD[((Int, Int), (Int, Int))] = data2.map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))

    val res: RDD[((Int, Int), Array[(Int, Int)])] = kv.combineByKey(
      //      createCombiner: V => C,
      //      mergeValue: (C, V) => C,
      //      mergeCombiners: (C, C) => C

      //第一条记录怎么放：
      (v1: (Int, Int)) => {
        //        Array(v1, (0, 0), (0, 0)) //数组长度 最好动态获取
        val intToInt: Array[(Int, Int)] = new Array[(Int, Int)](3)
        intToInt(0) = v1
        intToInt
      },
      //第二条，以及后续的怎么放：
      (oldv: Array[(Int, Int)], newv: (Int, Int)) => {
        //去重，排序
        var flg = 0 //  0,1,2 新进来的元素特征：  日 a)相同  1）温度大 2）温度小   日 b)不同

        for (i <- 0 until oldv.length) {

          if (oldv(i) == null) oldv(i) = (0, 0)

          if (oldv(i)._1 == newv._1) {
            if (oldv(i)._2 < newv._2) {
              flg = 1
              oldv(i) = newv //新值 温度大于旧值 则替换
            } else {
              flg = 2
            }
          }
        }
        if (flg == 0) { //不是同一天的数据,则 数组最后一位索引赋值 新值,再 排序 (则最后一位值肯定是当前中最小的值)
          oldv(oldv.length - 1) = newv
        }

        //        oldv.sorted
        scala.util.Sorting.quickSort(oldv)
        oldv

      },
      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
        //关注去重
        val union: Array[(Int, Int)] = v1.union(v2).distinct
        //    union.sorted
        scala.util.Sorting.quickSort(union)
        union
      }

    )
    res.map(x => (x._1, x._2.toList)).foreach(println)

   kv.mapPartitionsWithIndex((Index, iter) => {

     iter.map(e => (Index, e))

    }).foreach(println)


//    spark.stop()

    //第四代 --错误
    //    ((2018,3),CompactBuffer((11,18)))
    //    ((2019,5),CompactBuffer((21,33)))
    //    ((2018,4),CompactBuffer((23,22)))
    //    ((1970,8),CompactBuffer((8,32), (23,23)))
    //    ((2019,6),CompactBuffer((1,39), (1,38), (2,31)))
    //  用了groupByKey  容易OOM  取巧：用了spark 的RDD  sortByKey 排序
    //  没有破坏多级shuffle的key的子集关系 ,但 没去重
    //        val sorted: RDD[(Int, Int, Int, Int)] = data2.sortBy(t4=>(t4._1,t4._2,t4._4),false)
    //        val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = sorted.map(t4=>((t4._1,t4._2),(t4._3,t4._4)))
    //          .groupByKey()
    //        grouped.foreach(println)


    //第三代 --错误
    //    ((2018,3),CompactBuffer((11,18)))
    //    ((2019,5),CompactBuffer((21,33)))
    //    ((2018,4),CompactBuffer((23,22)))
    //    ((1970,8),CompactBuffer((23,23), (8,32)))
    //    ((2019,6),CompactBuffer((2,31), (1,39)))

    //  用了groupByKey  容易OOM  取巧：用了spark 的RDD 的reduceByKey 去重，用了sortByKey 排序
    //  注意：多级shuffle关注  后续书法的key一定得是前置rdd  key的子集
    //   全局排序之后 重新 shuffle 由于 第二个 key 不包含在第一个key 中 ,排序乱序
    //        val sorted: RDD[(Int, Int, Int, Int)] = data2.sortBy(t4=>(t4._1,t4._2,t4._4),false)
    //        val reduced: RDD[((Int, Int, Int), Int)] = sorted.map(t4=>((t4._1,t4._2,t4._3),t4._4)).reduceByKey(  (x:Int,y:Int)=>if(y>x) y else x )
    //        val maped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2=>((t2._1._1,t2._1._2),(t2._1._3,t2._2)))
    //        val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = maped.groupByKey()
    //        grouped.foreach(println)


    //第二代
        ((2018,3),List((11,18)))
        ((2019,5),List((21,33)))
        ((2018,4),List((23,22)))
        ((1970,8),List((8,32), (23,23)))
        ((2019,6),List((1,39), (2,31)))
        //用了groupByKey  容易OOM  取巧：spark rdd  reduceByKey 的取 max间接达到去重  让自己的算子变动简单点
//            val reduced: RDD[((Int, Int, Int), Int)] = data2.map(x=>((x._1,x._2,x._3),x._4)).reduceByKey((x:Int,y:Int)=>if(y>x) y else x )
//              reduced.map(t2=>((t2._1._1,t2._1._2),(t2._1._3,t2._2)))
//                      .groupByKey()
//                      .mapValues(arr=>arr.toList.sorted.take(2)).foreach(println)
    //


    //  第一代
    //    ((2018,3),List((11,18)))
    //    ((2019,5),List((21,33)))
    //    ((2018,4),List((23,22)))
    //    ((1970,8),List((8,32), (23,23)))
    //    ((2019,6),List((1,39), (2,31)))
    //  用了groupByKey 容易OOM   且自己的算子实现了函数：去重、排序
    //        val grouped = data2.map(t4=>((t4._1,t4._2),(t4._3,t4._4))).groupByKey()
    //        val res: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
    //          val map = new mutable.HashMap[Int, Int]()
    //          //获取同一天中 最高的温度
    //          arr.foreach(x => {
    //            if (map.get(x._1).getOrElse(0) < x._2) map.put(x._1, x._2)
    //          })
    //          //排序 获取 同一个月份最高的温度 高->低 排序
    //          map.toList.sorted(new Ordering[(Int, Int)] {
    //            override def compare(x: (Int, Int), y: (Int, Int)) = y._2.compareTo(x._2)
    //          })
    //        })
    //        res.foreach(println)

    //    sc.stop

//    while (true){
//
//    }
  }
}
