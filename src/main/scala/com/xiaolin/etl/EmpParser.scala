package com.xiaolin.etl

import com.xiaolin.utils.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

/**
  * 日志解析工具类
  */
object EmpParser {

  // schema
  val struct = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("salary", DoubleType),
    StructField("time", LongType),
    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  // parse log
  def parseLog(log: String,totals:LongAccumulator,errors:LongAccumulator): Row = {
    try {
      totals.add(1)
      val splits = log.split("\t")
      val id = splits(0).toInt
      val name = splits(1)

      val time = DateUtils.getTime(splits(2))
      val minute = DateUtils.parseToMinute(splits(2))
      val day = DateUtils.getDay(minute)
      val hour = DateUtils.getHour(minute)

      //val salary = splits(3).toDouble //非严格要求字段 数据不能扔掉
      var salary=0.0
      try{
        salary = splits(3).toDouble
      }

      Row(
        id, name,salary, time, day, hour
      )
    } catch {
      case e: Exception => e.printStackTrace()
        errors.add(1)
        Row(0)

    }
  }
}