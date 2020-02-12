package main.scala.com.xiaolin.huawei.ads

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 统计企业 维度指标总计
 * --数据来源: oucloud_Ads----companysday 日统计数据表
 * 按区域划分:管辖内 行政企业 业务企业(业务本地,业务外地,业务其他) 数量以及占比
 * @author linzhy
 */
object CompanysV2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.debug.maxToStringFields","100")
      .getOrCreate()

    var config2 = ConfigFactory.load()
    val ads_url = config2.getString("pg.oucloud_ads.url")
    val ads_user = config2.getString("pg.oucloud_ads.user")
    val ads_password = config2.getString("pg.oucloud_ads.password")

    //读取comapanysday表 信息
    val comdf = spark.read.format("jdbc")
      .option("url", ads_url)
      .option("dbtable","companys_day")
      .option("user", ads_user)
      .option("password", ads_password)
      .load()

    //根据天统计量,统计总指标
    comdf.createOrReplaceTempView("companys_day")

    val sql=
      """
        |SELECT
        |	coms.county_name,
        |	coms.xz_companys_sum,
        | coms.EPC_count,
        | concat(round(coms.EPC_count/coms.xz_companys_sum * 100,2),'%') epc_proportion,
        |	coms.company_construction_count,
        |	concat ( round( coms.company_construction_count / coms.xz_companys_sum * 100, 2 ), '%' ) construction_proportion,
        |	coms.company_labour_count,
        |	concat ( round( coms.company_labour_count / coms.xz_companys_sum * 100, 2 ), '%' ) labour_proportion,
        |	coms.company_local_count,
        |	concat ( round( coms.company_local_count / coms.xz_companys_sum * 100, 2 ), '%' ) local_proportion,
        |	coms.company_nonlocal_count,
        |	concat ( round( coms.company_nonlocal_count / coms.xz_companys_sum * 100, 2 ), '%' ) nonlocal_proportion,
        |	coms.other,
        |	concat ( round( coms.other / coms.xz_companys_sum * 100, 2 ), '%' ) other_proportion,
        |	coms.yw_companys_sum,
        |	coms.yw_company_local_count,
        |	concat ( round( coms.yw_company_local_count / coms.yw_companys_sum * 100, 2 ), '%' ) yw_local_proportion,
        |	coms.yw_company_nonlocal_count,
        |	concat ( round( coms.yw_company_nonlocal_count / coms.yw_companys_sum * 100, 2 ), '%' ) yw_nonlocal_proportion,
        |	coms.yw_other,
        |	concat ( round( coms.yw_other / coms.yw_companys_sum * 100, 2 ), '%' ) yw_other_proportion,
        | now() create_time
        |FROM
        |	(
        |	SELECT
        |		d.county_name,
        |		SUM ( d.xz_companys_sum ) xz_companys_sum,
        |   SUM ( d.EPC_count) EPC_count,
        |		SUM ( d.company_construction_count ) company_construction_count,
        |		SUM ( d.company_labour_count ) company_labour_count,
        |		SUM ( d.company_local_count ) company_local_count,
        |		SUM ( d.company_nonlocal_count ) company_nonlocal_count,
        |		SUM ( d.other ) other,
        |		SUM ( d.yw_companys_sum ) yw_companys_sum,
        |		SUM ( d.yw_company_local_count ) yw_company_local_count,
        |		SUM ( d.yw_company_nonlocal_count ) yw_company_nonlocal_count,
        |		SUM ( d.yw_other ) yw_other
        |	FROM
        |		companys_day d
        |	GROUP BY
        |	county_name
        |	) coms
        |
        |""".stripMargin
    val companyDF = spark.sql(sql)
    companyDF.show(100)
    //left join 保存数据
    companyDF.write.format("jdbc")
        .mode(SaveMode.Append)
        .option("dbtable","companys_index")
        .option("url",ads_url)
        .option("user",ads_user)
        .option("password",ads_password)
        .save()

    spark.stop()
  }

}
