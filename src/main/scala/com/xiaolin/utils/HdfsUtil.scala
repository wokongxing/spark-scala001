package com.xiaolin.utils

import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsUtil {
  /**
    * 文件存在则删除文件
    * @param path
    * @param fileSystime
    */
  def deleteFile(path:String,fileSystime: FileSystem): Unit ={
    if (fileSystime.exists(new Path(path))){
      fileSystime.delete(new Path(path),true)
    }
  }

  /**
   * 依据读取的文件数 以及文件大小 重新设置合理的分区
   * @param flieSystem
   * @param path 路径
   * @param size block块的大小
   * @return
   */
  def  getCoalescesNum(flieSystem:FileSystem,path:String,size:Long=128) ={
    var partitions:Long=0
    flieSystem.globStatus(new Path(path)).map(x=>{
      println(x.getPath)
      partitions += x.getLen

    })
    ((partitions/1024/1024)/size).toInt+1
  }

}
