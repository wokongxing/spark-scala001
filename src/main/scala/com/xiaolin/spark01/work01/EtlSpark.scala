package main.scala.com.xiaolin.spark01.work01

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import main.scala.com.xiaolin.spark01.entity.IpInfo
import main.scala.com.xiaolin.spark01.utils.{ContextUtils, IpUtil}
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._

object EtlSpark {

  private val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss Z")
  def main(args: Array[String]): Unit = {
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName)
    var ipInfo = null;
    val iplist = sc.textFile("D:\\IDEAspaces\\sparkscala\\data\\ip.txt")

    val iplists: List[IpInfo] = iplist.map(x => {
      val splits = x.split("\\|")
      new IpInfo(splits(2).toLong, splits(3).toLong, splits(6), splits(7), splits(9));
    }).collect().toList

    val logrdd = sc.textFile("D:\\IDEAspaces\\sparkscala\\data\\access02.log")
      logrdd.map(x=>{
        val spilts = x.split("\t")
        val domain = spilts(0);
        val time = spilts(1);
        val flow = spilts(2);
        val ip = spilts(3);

        try {
          val date = format.parse(time.substring(1, time.length - 1))
          //解析日期
          val calendar = Calendar.getInstance
          calendar.setTime(date)
          val year = calendar.get(Calendar.YEAR)
          val month = calendar.get(Calendar.MONTH) + 1
          val day = calendar.get(Calendar.DATE)
          // 去除掉不符合要求的流量字段
          val sizes = flow.toLong
          //解析ip
          val ipInfo = IpUtil.searchIp(iplists, ip)

          (domain,date,year,month,day,sizes,ipInfo.province,ipInfo.city,ipInfo.isp)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      })
  }
}
