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

}
