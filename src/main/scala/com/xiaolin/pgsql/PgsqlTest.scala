package main.scala.com.xiaolin.pgsql

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object PgsqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val config = ConfigFactory.load()
    val url = config.getString("pg.default.url")
    val user = config.getString("pg.default.user")
    val password = config.getString("pg.default.password")

    //获取企业表基本信息
    val company = spark.read.format("jdbc")
                        .option("url",url)
                        .option("dbtable","corp_basic_info")
                        .option("user",user)
                        .option("password",password)
                        .load()
    //company.



    spark.stop()
  }

}
