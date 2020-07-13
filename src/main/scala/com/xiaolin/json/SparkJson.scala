package main.scala.com.xiaolin.json

import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark._
object SparkJson {

  def main(args: Array[String]): Unit = {


    val str=
      """
        |{"dto":{"method":"labourer.import","version":"1.0","appid":"1001","format":"json","timestamp":1575595340057,"nonce":"ccb5ce4a892e4b5cab793cd72fed1204","sign":"56ca0820990cbccdd986bbbe8be281ca34b9b123adabd01ec347a9ba7edc29a7","data":"[{\"real_name\":\"黄传卫\",\"user_photo\":null,\"gender\":0,\"birthday\":\"1969-12-10\",\"id_card_no\":\"iLmdkhNgALbYkVd7rRmzO9R+V7PSzzH1DE714Ijshgg=\",\"start_date\":null,\"expiry_date\":null,\"mobilephone\":\"15957725053\",\"province_code\":null,\"city_code\":null,\"county_code\":null,\"address\":null,\"current_address\":null,\"nation\":\"1\",\"married\":\"02\",\"bank_code\":null,\"bank_no\":null,\"job_date\":\"2019-12-06\",\"worktype_code\":\"240\",\"group_name\":\"木工班组\",\"project_name\":\"乐清市中心区B-C1-2出让地块建设项目\"},{\"real_name\":\"郎啟军\",\"user_photo\":null,\"gender\":0,\"birthday\":\"1966-10-17\",\"id_card_no\":\"v3COIPRuw5+qbv2MSBBQGp3xIQUX/xcwjxj4A+KmY6M=\",\"start_date\":null,\"expiry_date\":null,\"mobilephone\":\"13968801091\",\"province_code\":null,\"city_code\":null,\"county_code\":null,\"address\":null,\"current_address\":null,\"nation\":\"1\",\"married\":\"01\",\"bank_code\":null,\"bank_no\":null,\"job_date\":\"2019-12-06\",\"worktype_code\":\"240\",\"group_name\":\"木工班组\",\"project_name\":\"新桥街道山前村、三浃村、西湖村城中村改造安置房工程代建开发项目\"},{\"real_name\":\"郎啟军\",\"user_photo\":null,\"gender\":0,\"birthday\":\"1966-10-17\",\"id_card_no\":\"v3COIPRuw5+qbv2MSBBQGp3xIQUX/xcwjxj4A+KmY6M=\",\"start_date\":null,\"expiry_date\":null,\"mobilephone\":\"13968801091\",\"province_code\":null,\"city_code\":null,\"county_code\":null,\"address\":null,\"current_address\":null,\"nation\":\"1\",\"married\":\"01\",\"bank_code\":null,\"bank_no\":null,\"job_date\":\"2019-12-06\",\"worktype_code\":\"240\",\"group_name\":\"木工班组\",\"project_name\":\"新桥街道山前村、三浃村、西湖村城中村改造安置房工程代建开发项目\"}]"}}
        |
        |""".stripMargin

//    val spark = SparkSession.builder().master("local[2]").appName("json").getOrCreate()
//
//    val anotherPeopleDS : Dataset[String]=spark.sparkContext.parallelize("""{"name":"Yin","address":[{"city":"Columbus","state":"http://www.tom.com"},{"city":"Columbus2","state":"http://www.tom.com.cn"}]}"""::Nil)
//
//    val resultDS = spark.read.json(anotherPeopleDS)
//
//    val result1DS = resultDS.select(resultDS("name"),functions.explode(resultDS("address"))).toDF("name","address")
//    val result2DS = result1DS.select("name","address.city","address.state").show(false)

  }

}
