package com.xiaolin.sparksql.work

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object sparksql01 {

//text多列保存-->将需要保存的数据字段,拼接成一个字段
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile("data/test.txt")
    ds.map(x=>{
      val splits = x.split(",")
      splits(0)+","+splits(1)
    }).write.mode(SaveMode.Overwrite).format("text").save("outdata/sparksql/work01")


    spark.stop()
  }

}
