package main.scala.com.xiaolin.hive

import org.apache.spark.sql.{SparkSession, functions}

/** window 访问 集群中的 hive
 *  1.  hive 开启metastore -- hive --service metastore
 *  2.  spark 配置 hive.metastore.uris
 *  3. 放开端口 50010 9083
 *  4. 放开hdfs权限 或者 设置 hadoop用户
 *  5. 设置 dfs.client.use.datanode.hostname=true 内外网通信
 */
object SparksqlHive {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("dfs.client.use.datanode.hostname", "true") //以域名的方式返回 访问 相互通信
      .config("hive.metastore.uris", "thrift://hadoop001:9083")
      .enableHiveSupport() //启动hive读取配置文件中的
      .getOrCreate()


    spark.sparkContext.setLogLevel("INFO")
    val sql2 ="show tables"
    //ddl
    val sql ="create table person7 (id int, code string, name string)"
    //dml
    val sql1 =
      """
        | insert into test values (4,'xiaolin'),(2,'xiaowang'),(3,'xiaoli')
        |""".stripMargin

    val sql3 ="select * from test"
//
    spark.sql(sql)
    spark.sql(sql1)
    spark.sql(sql3).show()
    spark.sql(sql2).show()


    spark.stop();
  }
}
