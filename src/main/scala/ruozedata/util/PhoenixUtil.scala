package ruozedata.util

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by ruozedata-J.
  * www.ruozedata.com
  */
class PhoenixUtil {

  // zk地址
  val connstr = "jdbc:phoenix:ruozedata001,ruozedata002,ruozedata003:2181/hbase"
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
