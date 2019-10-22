package com.xiaolin.sparksql.sql01

import org.apache.spark.sql.SparkSession

/**
  * 讲师：PK哥   交流群：545916944
  */
object CatalogApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("CatalogApp")
      .getOrCreate()

    val catalog = spark.catalog
    catalog.listDatabases()
    catalog.listTables("ruozedata_ba").show(false)
    catalog.listFunctions().show(false)
    catalog.listColumns("ruozedata_ba.platform_stat").show(false)



    spark.stop()
  }
}
