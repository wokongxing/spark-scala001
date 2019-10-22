package com.xiaolin.scala04

import java.sql.DriverManager

/**
  * 讲师：PK哥
  */
object ScalaJDBCApp {

  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://ruozedata001:3306/ruozedata_d7"
    val user = "root"
    val password = "ruozedata"

    val connection = DriverManager.getConnection(url, user, password)
    val statement = connection.prepareStatement("select * from DBS")
    val rs = statement.executeQuery()

    while(rs.next()){
      val location = rs.getString("DB_LOCATION_URI")
      val name = rs.getString("NAME")

      println(location + " \t " + name)
    }

    //todo... close resource
  }

}
