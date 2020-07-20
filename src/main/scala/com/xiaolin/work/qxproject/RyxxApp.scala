package xiaolin.work.qxproject

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 *
 * 获取企薪人员信息
 */
object RyxxApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSessionApp")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("ID",StringType),
      StructField("HeadImageUrl",StringType),
      StructField("BirthPlaceCode",StringType),
      StructField("UrgentLinkMan",StringType),
      StructField("UrgentLinkManPhone",StringType),
      StructField("NegativeIDCardImageUrl",StringType),
      StructField("StartDate",StringType),
      StructField("PositiveIDCardImageUrl",StringType),
      StructField("GrantOrg",StringType),
      StructField("ExpiryDate",StringType),
      StructField("Address",StringType),
      StructField("Nation",StringType),
      StructField("birthday",StringType),
      StructField("JoinedTime",StringType),
      StructField("MaritalStatus",StringType),
      StructField("gender",StringType),
      StructField("CellPhone",StringType),
      StructField("PoliticsType",StringType),
      StructField("EduLevel",StringType),
      StructField("IsJoined",StringType),
      StructField("Name",StringType),
      StructField("IDCardNumber",StringType)
    ))
//获取参数配置
    val config = ConfigFactory.load()
    val url = config.getString("db.sptn.url")
    val user = config.getString("db.sptn.user")
    val password = config.getString("db.sptn.password")
    val driver = config.getString("db.sptn.driver")
    import spark.implicits._
    val ryrdd = spark.sparkContext.textFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_ryxx.txt")
      .map(x => {
        val splits = x.toString().split(",")
        if (splits.length == 22) {
          Row(splits(0).toString, splits(1).toString, splits(2).toString, splits(3).toString, splits(4).toString, splits(5).toString,
            splits(6).toString, splits(7).toString, splits(8).toString, splits(9).toString, splits(10).toString, splits(11).toString,
            splits(12).toString, splits(13).toString, splits(14).toString, splits(15).toString, splits(16).toString, splits(17).toString,
            splits(18).toString, splits(19).toString, splits(20).toString, splits(21).toString)
        } else {
          Row(99)
        }
      }).filter(_.get(0)!=99)

    spark.createDataFrame(ryrdd,schema)
      .write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("dbtable","temp_ryxx2")
      .option("user",user)
      .option("password",password)
      .save()
//      .coalesce(1).foreach(x=>println(x))
//      .toJavaRDD.rdd.saveAsTextFile("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_ryxx_error.txt")
//      .write.mode(SaveMode.Overwrite).format("text")
//      .save("file:///C:\\Users\\linzhy\\Desktop\\huawei\\企薪临时\\jkm_ryxx_error.txt")



    spark.stop()
  }
}
