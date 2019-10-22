package com.xiaolin.Test01


import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object SparkSqlApi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    //val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._
    val rdd = spark.read.format("text").load("outdata/access/01/part*")
    val df = rdd.map(x=>{
      val splits = x.toString().split(",")
      (splits(1),splits(5),splits(6),splits(7))
    }).toDF("platform", "traffic", "province", "city")


    df.createOrReplaceGlobalTempView("logs")
    val sql2 =
      """
        |
        |select a.* from
        |(
        |select t.*,
        |row_number() over(partition by platform order by cnt desc nulls last) as row_,
        |rank() over(partition by platform order by cnt desc nulls last) as rank_,
        |dense_rank() over(partition by platform order by cnt desc nulls last) as dense_
        |from
        |(select platform,province,count(1) cnt from global_temp.logs group by platform,province) t
        |) a
        |
      """.stripMargin

    spark.sql(sql2).show()
    //fileSystem.close();
    spark.stop();
  }

}
