package main.scala.com.xiaolin.hbase


import com.xiaolin.utils.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import com.xiaolin.flink.utils.HbaseUtil

import scala.collection.mutable.ListBuffer

/**
 * 测试用例:
 *    数据从json中,以 Hfile方式导入Hbase
 *    BulkLoad
 *
 * @author linzhy
 */
object DataHiveToHbaseBulkLoad {

  val zookeeperQuorum = "hadoop001:2181"
  val dataSourcePath = "data/data-test.json"
  val hdfsRootPath = "hdfs://hadoop001:9000"
  val hFilePath = "hdfs://hadoop001:9000/bulkload/hfile/"
  val tableName = "person"
  val familyName = "cf1"


  //设置用户
  System.setProperty("user.name", "hadoop")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //必须序列化
      .getOrCreate()

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", hdfsRootPath)
    hadoopConf.set("dfs.client.use.datanode.hostname","true");
    val fileSystem = FileSystem.get(hadoopConf)

    val hbaseConf = HBaseConfiguration.create(hadoopConf)
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //如果导入数据量过大,可以适当修改默认值32
    hbaseConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","3200")

    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin
    val regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName))

    // 0. 准备程序运行的环境
    // 如果 HBase 表不存在，就创建一个新表
    HbaseUtil.createTable(tableName,familyName,admin)

    // 如果存放 HFile文件的路径已经存在，就删除掉
    HdfsUtil.deleteFile(hFilePath,fileSystem)

    // 1. 清洗需要存放到 HFile 中的数据，rowKey 一定要排序，否则会报错：
    // java.io.IOException: Added a key not lexically larger than previous.

    val dataFrame = spark.read.json(dataSourcePath).select("sessionid","sdkversion","requestdate","email","title")

    val columns = dataFrame.columns.dropWhile(_=="sessionid").sortBy(x=>(x,true)) //排序

    //数据处理
    val data = dataFrame.rdd.map(jsonstr => {
      val rowkey = jsonstr.getAs[String]("sessionid")
      val ik = new ImmutableBytesWritable(Bytes.toBytes(rowkey))
      var linkedList = new ListBuffer[KeyValue]()
      columns.map(column => {
        val columnValue = jsonstr.getAs[String](column).toString
        val kv = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(columnValue))
        linkedList.append(kv)
      })
      (ik, linkedList)
    }).flatMapValues(s => { //打散 排序
        val value = s.iterator
        value
      }
    ).sortByKey()

     // 2. Save Hfiles on HDFS
    val table = hbaseConn.getTable(TableName.valueOf(tableName))
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)

    data.saveAsNewAPIHadoopFile(
      hFilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    //  3. Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path(hFilePath), admin, table, regionLocator)

    hbaseConn.close()
    fileSystem.close()
    spark.stop()
  }

}
