package main.scala.com.xiaolin.huawei

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
object SparkEs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("estest")
      .master("local[2]")
      //.config("pushdown","true")
      .config("es.nodes","47.99.208.29")
     // .config("es.index.auto.create", "true") //创建索引
      .config("es.port","9201")
      .config("es.nodes.wan.only","true")
      .getOrCreate()

//    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//    spark.sparkContext.makeRDD(Seq(numbers,airports)).saveToEs("spark/docs")

//    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
//    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
//    val json3 = """{"participants" : 15, "airport" : "O22TP"}"""
//    spark.sparkContext.makeRDD(Seq(json1,json2,json3)).saveJsonToEs("spark/docs")

    //core
    val index = s"test/product"
    val index2 = s"ousutec-attendance-debug/clockin-record-debug"
//    val query =
//      """
//        |{
//        |  "query": {
//        |    "bool": {
//        |      "should": [
//        |        {"match": { "name": "小"}},
//        |        {"match": { "name": "为"}}
//        |      ]
//        |    }
//        |  }
//        |}
//        |""".stripMargin
//    val query2 =
//      """
//        |{
//        |  "query": {
//            "bool": {
//        |      "must": [
//        |       {"match": { "direct": "1"}},
//        |        {"match": { "pid": "81630"}}
//        |      ]
//        |    }
//        |  },
//        |  "size": 20,
//        |  "sort": [
//        |    {
//        |      "record_time": {
//        |        "order": "desc"
//        |      }
//        |    }
//        |  ]
//        |}
//        |""".stripMargin
//
//    val testrdd: RDD[(String, collection.Map[String, AnyRef])] = spark.sparkContext.esRDD(index2, query2)
//    testrdd.foreach(println)

    //sql
    val frame = spark.esDF(index2).where("record_time > to_timestamp('2020-01-03')")
    //frame.cache()

//    frame.createOrReplaceTempView("attendance")
//    val sql=
//      """
//        |select * from attendance
//        |""".stripMargin
//
//    val dataFrame = spark.sql(sql)
//    dataFrame.printSchema()
    frame.show(10)
    Thread.sleep(100000)
    spark.stop()
  }
}
