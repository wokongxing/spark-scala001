package com.xiaolin.structuredStreaming

import org.apache.spark.sql.SparkSession

object ReadStream {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[2]")
      .getOrCreate()

    val lines = spark.readStream
      .format("socket")
      .option("host", "hadoop001")
      .option("port", 9999)
      .load()
    import spark.implicits._
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
