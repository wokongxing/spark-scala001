package com.xiaolin.spark01.work03
import main.scala.com.xiaolin.spark01.utils.ContextUtils
import main.scala.com.xiaolin.spark01.utils.ImplicitAspect._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
object SparkWork001 {
//"""
//  Android类型的只要：ruozedata1 182.86.190.207  2271 江西  景德镇
//  iOS类型的只要：ruozedata1 182.86.190.207  2271 江西
//  Symbian类型的只要：182.86.190.207  2271 江西  景德镇
//  ==> 有能力的扩展成可以自定义根据域名配置来生成
//"""
  def main(args: Array[String]): Unit = {
    val sc = ContextUtils.getSparkContext(this.getClass.getSimpleName)
    var outpath = "outdata/spark01";

    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (fileSystem.exists(new Path(outpath))){
      fileSystem.delete(new Path(outpath),true)
    }

    val dataRdd = sc.textFile("data/etl_access.log")
      .filter(x=>x.split("\t").length==10)
      .map(x=> {
        val splits = x.split("\t")
          val domain = splits(0)
          val ip = splits(1)
          val flow = splits(2)
          val pro = splits(4)
          val city = splits(5)
          val isp = splits(6)
          val year = splits(7)
          val month = splits(8)
          val day = splits(9)
        (domain,(domain,ip,flow,pro,city,isp,year,month,day))
      }).coalesce(1,true)
      .saveAsHadoopFile(outpath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat] )
      //注:saveAsHadoopFile 必须是key-value类型 ;saveAsTextFiled底层也是调用saveAsHadoopFile方法的

    sc.stop()
  }

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any,Any]{
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
      //已key作为分目录
      val fliename = key+"/"+name
      fliename
    }

    override def generateActualKey(key: Any, value: Any): Any = {
      //输出数据 不需要key
      NullWritable.get()
    }

    override def generateActualValue(key: Any, value: Any): Any = {
      //根据不同的key ,输出不同的value 待优化--->可以从数据库获取想要输出的value
      val splits = value.toString.split(",")
      val sb = new StringBuffer
      key match {
        case "www.ruozedata.com" =>{
          sb.append(splits(0)).append("\t")
            .append(splits(1)).append("\t")
            .append(splits(3))
          sb
        }
        case "www.baidu.com" =>{
          sb.append(splits(0)).append("\t")
            .append(splits(1)).append("\t")
            .append(splits(2))
          sb
        }
        case "www.alibaba.com" =>{
          sb.append(splits(0)).append("\t")
            .append(splits(2)).append("\t")
            .append(splits(3)).append("\t")
            .append(splits(4))
          sb
        }
        case _ =>NullWritable.get()
      }
    }

  }

}
