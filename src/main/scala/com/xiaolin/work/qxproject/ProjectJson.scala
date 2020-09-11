package com.xiaolin.work.qxproject


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object ProjectJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[4]").appName("json")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    import spark.implicits._
    val dataFrame = spark.read.option("inferSchema", "true").format("json").load("D:\\IdeaProjects\\spark-scala001\\data\\project1.json")
    val transDF = dataFrame.select($"code",explode($"data")).toDF("code","datas")
    val dataFrame1 = transDF.select(
      $"datas.PRJNAME".as("PRJNAME"),
      $"datas.BUILDCORPNAME".as("BUILDCORPNAME"),
      $"datas.BUILDCORPCODE".as("BUILDCORPCODE"),
      $"datas.BUILDCORPADDRESS".as("BUILDCORPADDRESS"),
      $"datas.BUILDERCORPLEADER".as("BUILDERCORPLEADER"),
      $"datas.BUILDERCORPLEADERIDCARD".as("BUILDERCORPLEADERIDCARD"),
      $"datas.BUILDERCORPLEADERPHONE".as("BUILDERCORPLEADERPHONE"),
      explode($"datas.SUBSET")
    ).toDF("PRJNAME", "BUILDCORPNAME", "BUILDCORPCODE", "BUILDCORPADDRESS","BUILDERCORPLEADER","BUILDERCORPLEADERIDCARD","BUILDERCORPLEADERPHONE", "SUBSETS")

    val dataFrame2 = dataFrame1.select(
      $"PRJNAME",
      $"BUILDCORPNAME",
      $"BUILDCORPCODE",
      $"BUILDCORPADDRESS",
      $"BUILDERCORPLEADER",
      $"BUILDERCORPLEADERIDCARD",
      $"BUILDERCORPLEADERPHONE",
      $"SUBSETS.CORPNAME" as "CORPNAME",
      $"SUBSETS.ROWGUID" as "ROWGUID",
      $"SUBSETS.CORPTYPENUM" as "CORPTYPENUM",
      $"SUBSETS.CORPCODE" as "CORPCODE",
      $"SUBSETS.CORPLEADER" as "CORPLEADER",
      $"SUBSETS.CORPLEADERCARDTYPE" as "CORPLEADERCARDTYPE",
      $"SUBSETS.CORPLEADERCARDNUM" as "CORPLEADERCARDNUM",
      $"SUBSETS.CORPLEADERPHONE" as "CORPLEADERPHONE"
    )
    val sqlText =
      """
        |select PRJNAME,CORPNAME,CORPCODE,CORPTYPENUM,CORPLEADER,CORPLEADERCARDTYPE,CORPLEADERCARDNUM,CORPLEADERPHONE from (
        |select PRJNAME,CORPNAME,CORPCODE,CORPTYPENUM,CORPLEADER,CORPLEADERCARDTYPE,CORPLEADERCARDNUM,CORPLEADERPHONE from project_t
        |union
        |select
        |PRJNAME,
        |BUILDCORPNAME as CORPNAME,
        |BUILDCORPCODE as CORPCODE,
        | '建设单位' as CORPTYPENUM,
        | BUILDERCORPLEADER as CORPLEADER,
        | '身份证' as CORPLEADERCARDTYPE,
        | BUILDERCORPLEADERIDCARD as CORPLEADERCARDNUM,
        | BUILDERCORPLEADERPHONE as CORPLEADERPHONE from project_t
        | group by PRJNAME,BUILDCORPNAME,BUILDCORPCODE,BUILDERCORPLEADER,BUILDERCORPLEADERIDCARD,BUILDERCORPLEADERPHONE
        |
        |)
        |
        |""".stripMargin


    //获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.sptn.url")
    val user = config.getString("db.sptn.user")
    val password = config.getString("db.sptn.password")

//    val dataFrame3 = spark.sql(sqlText)
//    dataFrame3.show(100)
//    dataFrame3.write
//          .mode(SaveMode.Overwrite)
//          .format("jdbc")
//          .option("url",url)
//          .option("dbtable","temp_project_five")
//          .option("user",user)
//          .option("password",password)
//          .save()
    dataFrame2.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("dbtable","temp_project_five1")
      .option("user",user)
      .option("password",password)
      .save()



    spark.stop()

  }

}
