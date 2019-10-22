package main.scala.com.xiaolin.spark01.work01

import java.text.SimpleDateFormat
import java.util.Calendar

import com.xiaolin.utils.{HdfsUtil, Ip2Util}
import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SparkSession

object EtlSpark {

  private val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss Z")

  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
  import spark.implicits._
    val logrdd = spark.read.json("D:\\IDEAspaces\\sparkscala\\data\\access03.log")
    //logrdd.rdd.map(x=>x.toString()).printInfo()

    val outpath = "outdata/access/01";
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    HdfsUtil.deleteFile(outpath,fileSystem);

    logrdd.rdd.map(x=>{
      val spilts = x.toString().split(",")
      val domain = spilts(0).substring(1,spilts(0).length);
      val flow = spilts(1);
      val ip = spilts(2);
      val plat = spilts(3);
      val time = spilts(4);
      try {
        if (StringUtils.isNumeric(flow)){// 去除掉不符合要求的流量字段
          val date = format.parse(time.substring(1, time.length - 2))
          //解析日期
          val calendar = Calendar.getInstance
          calendar.setTime(date)
          val year = calendar.get(Calendar.YEAR)
          val month = calendar.get(Calendar.MONTH) + 1
          val day = calendar.get(Calendar.DATE)
          val sizes = flow.toLong
          //解析ip
          val ipInfo = Ip2Util.getAaddressByIp(ip)
          val splits = ipInfo.split("\\|")
          var province = "";
          var city = "";
          var isp = "";
          if (splits.length==5){
            province = splits(2)
            city = splits(3)
            isp = splits(4)
          }
          (domain,plat,year,month,day,sizes,province,city,isp)
        }else{
          (NullWritable.get())
        }

      } catch {
        case e: Exception =>
          e.printStackTrace()
          (NullWritable.get())
      }
    }).filter(x=>x!=NullWritable.get()).coalesce(1).saveAsTextFile(outpath)

    fileSystem.close();
    spark.stop();

  }
}
