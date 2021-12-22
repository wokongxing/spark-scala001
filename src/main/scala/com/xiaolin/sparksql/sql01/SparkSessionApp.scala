package com.xiaolin.sparksql.sql01

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  *
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSessionApp")
      .getOrCreate()

      text(spark)

    //json(spark)

    //csv(spark)

    //jdbc(spark)
//    jdbc2(spark)


    spark.stop()
  }

  def text(spark: SparkSession) = {
    import spark.implicits._
    //val df: DataFrame = spark.read.format("text").load("data/test.txt")
    //spark.read.text("data/people.txt").show()
    val ds: Dataset[String] = spark.read.textFile("data/test.txt")
//    ds.show()

    val resultDS = ds.map(x => {
      val splits = x.split(",")
      (splits(0), splits(1))
      //splits(0)
    })

    resultDS.write.option("compression","lz4").mode(SaveMode.Overwrite).format("text").save("outdata/sparksql/test")

//    df.rdd.map(x => {
//      val tmp = x.getString(0)
//      val splits = tmp.split(",")
//      (splits(0))
//    }).foreach(println)

  }

  def json(spark: SparkSession) = {
    import spark.implicits._
    val df = spark.read.format("json").load("data/init/access.log")
    df.createTempView("view_data")
    df.printSchema()
    println("........")
    //df.select("appId","platform","traffic","user")
    val resultDF = df.select(df("appId"))
      .filter(df("user") === "ruozedata73")
    //.show(10)
    //.where('user === "ruozedata73").show(10)
    resultDF.write.mode("overwrite").format("json").save("out")
  }

  def csv(spark: SparkSession) = {
    import spark.implicits._

    val df = spark.read.option("header","true")
        .option("sep",",")
      .format("csv").load("data/area.csv")
    df.printSchema()
    val resultdf = df.select("AREAID","PAREAID","AREAPY","ORDERID","ISAPPLY","MODTIME")

    //获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")

    val jdbcRdd = resultdf.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url",url)
      .option("dbtable","SYS_AREA")
      .option("user",user)
      .option("password",password)
      .save()

  }

  def jdbc(spark: SparkSession) = {
    import spark.implicits._

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://ruozedata001:3306")
      .option("dbtable", "ruozedata_ba.platform_stat")
      .option("user", "root")
      .option("password", "ruozedata")
      .load()

//    jdbcDF.show()

    jdbcDF.filter('platform === "Android")
      .write.format("jdbc")
      .option("url", "jdbc:mysql://ruozedata001:3306")
      .option("dbtable", "ruozedata_ba.platform_stat_2")
      .option("user", "root")
      .option("password", "ruozedata")
      .save()
  }

  def jdbc2(spark: SparkSession) = {
    import spark.implicits._

    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val srcTable = config.getString("db.default.srctable")
    val targetTable = config.getString("db.default.targettable")

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", srcTable)
      .option("user", user)
      .option("password", password)
      .load()

    //    jdbcDF.show()

    jdbcDF.filter('platform === "Android")
      .write.format("jdbc")
      .option("url", url)
      .option("dbtable", targetTable)
      .option("user", user)
      .option("password", password)
      .save()
  }



}
