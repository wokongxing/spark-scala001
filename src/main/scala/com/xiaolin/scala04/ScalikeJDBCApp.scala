package com.xiaolin.scala04



/**
  * 讲师：PK哥
  */
object ScalikeJDBCApp {

//  def insert(): Unit = {
//    DB.autoCommit({
//      implicit session => {
//        SQL("insert into offset_storage(topic,groupid,partitions,offset) values(?,?,?,?)")
//          .bind("pk","test-pk-group",3, 8)
//          .update().apply()
//      }
//    })
//  }
//
//  def update(): Unit = {
//    DB.autoCommit({
//      implicit session => {
//        SQL("update offset_storage set offset=? where topic=? and groupid=? and partitions=?")
//          .bind(18, "pk","test-pk-group",3)
//          .update().apply()
//      }
//    })
//  }
//
//  def query(): Unit = {
//    val offsets = DB.readOnly({
//      implicit session => {
//        SQL("SELECT * FROM offset_storage").map(rs => {
//          Offset(
//            rs.string("topic"),
//            rs.string("groupid"),
//            rs.int("partitions"),
//            rs.long("offset")
//          )
//        }).list().apply()
//      }
//    })
//    offsets.foreach(println)
//  }
//
//  def delete(): Unit = {
//    DB.autoCommit({
//      implicit session => {
//        SQL("delete from offset_storage  where partitions=?")
//          .bind(3)
//          .update().apply()
//      }
//    })
//  }
//
//  def transaction(): Unit = {
//    DB.localTx({
//      implicit session => {
//
//        SQL("delete from offset_storage  where partitions=?")
//          .bind(2)
//          .update().apply()
//
////        1/0
//
//        SQL("delete from offset_storage  where partitions=?")
//          .bind(1)
//          .update().apply()
//      }
//    })
//  }
//
//  def main(args: Array[String]): Unit = {
//    DBs.setupAll() // 解析配置文件
//
//    //insert()
////    update()
////    query()
////     delete()
//    transaction()
//  }
//
}
//case class Offset(topic:String, groupId:String, partitionId:Int, offset:Long)
