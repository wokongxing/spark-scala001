package main.scala.com.xiaolin.hive

import org.apache.spark.sql.SparkSession

object SparksqlHive {
  def main(args: Array[String]): Unit = {
    val satrt_time = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .enableHiveSupport() //启动hive读取配置文件中的
      .getOrCreate()
//    val sql = "show tables"
//    val sql2 ="select * from employee"
    import spark.implicits._
    val df = spark.read.format("text").load("data/datetime.text")
        .map(x=>{
          val splits = x.toString().split(",")
          splits
        }).toDF()

    df.printSchema()
    df.show(100)

//    spark.sql(sql2).show(10)

    val end_time = System.currentTimeMillis()
    println(end_time-satrt_time )
    println(spark.sparkContext.defaultParallelism )
    spark.stop();
  }
}
