package ruozedata.util

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import com.google.common
import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by ruozedata-J.
  * www.ruozedata.com
  */
class PhoenixUtil {

  // zk地址
//  val connstr = "jdbc:phoenix:hadoop001,ruozedata002,ruozedata003:2181/hbase"
  val connstr = "jdbc:phoenix:hadoop001:2181/hbase"
  val properties = new Properties
  properties.setProperty("hbase.rpc.timeout", "600000")
  properties.setProperty("hbase.client.scanner.timeout.period", "600000")
  properties.setProperty("dfs.client.socket-timeout", "600000")
  properties.setProperty("phoenix.query.keepAliveMs", "600000")
  properties.setProperty("phoenix.query.timeoutMs", "3600000")

  //支持高可靠
  val conn = DriverManager.getConnection(connstr,properties)
  conn.setAutoCommit(false)

  var pstmt: PreparedStatement = null
  var rs: ResultSet = null
  val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")

  //保存到HBase
  def saveToHBase(sqlstr: String) = {
    try {
      pstmt = conn.prepareStatement(sqlstr)
      pstmt.executeUpdate()

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  //查询 将ResultSet结果返回
  def searchFromHBase(sqlstr: String): ResultSet = {
    try {
      pstmt = conn.prepareStatement(sqlstr)
      rs = pstmt.executeQuery()
      rs
    } catch {
      case e: Exception =>
        println(e.getMessage)
        rs = null
        rs
    }
  }


  def closeCon() = {
    try {
      if (conn != null)
        conn.commit()
      conn.close()
      if (pstmt != null)
        pstmt.close()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }



}



object PhoenixUtil{

  def main(args: Array[String]): Unit = {
    //获取 phoenix 主键
    val sql =
      """
        |select
        |TENANT_ID TABLE_CAT,
        |TABLE_SCHEM,
        |TABLE_NAME ,
        |COLUMN_NAME,
        |KEY_SEQ,
        |PK_NAME,
        |CASE WHEN SORT_ORDER = 1 THEN 'D' ELSE 'A' END ASC_OR_DESC,
        |ExternalSqlTypeId(DATA_TYPE) AS DATA_TYPE,
        |SqlTypeName(DATA_TYPE) AS TYPE_NAME,
        |COLUMN_SIZE,
        |DATA_TYPE TYPE_ID,
        |VIEW_CONSTANT
        |from SYSTEM."CATALOG" where
        |TABLE_SCHEM = 'workdb'
        |and TABLE_NAME = 'test2'
        |and COLUMN_NAME is not null
        |and COLUMN_FAMILY is null
        |order by TENANT_ID,TABLE_SCHEM,TABLE_NAME ,COLUMN_NAME
        |""".stripMargin;
    val phoenixConn = new PhoenixUtil()
    val rs = phoenixConn.searchFromHBase(sql)
    while (rs.next()){

      println(rs.getString("table_name"))
      println(rs.getString("column_name"))
    }
  }
}

