package com.xiaolin.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-12-17 11:23
 **/
public class apiTest {
    public static final String HDFS_PATH="hdfs://hadoop001:9000";
    private  static Configuration conf=null;
    private  static FileSystem fs =null;

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname","true");
        fs = FileSystem.get(new URI(HDFS_PATH), conf, "root");

        Path outf  = new Path("/test2/dept.txt");
        Path intf  = new Path("./data/dept.txt");

//        if (fs.exists(outf)){
//            //删除
//            fs.delete(outf,true);
//        }
//        //创建
//        fs.mkdirs(outf);
//        //本地文件拷贝到hdfs
//        fs.copyFromLocalFile(intf,outf);

//        FSDataInputStream fsDataInputStream = fs.open(outf);
//
//        fsDataInputStream.seek(10);
//        System.out.println((char) fsDataInputStream.readByte());
//        System.out.println((char) fsDataInputStream.readByte());
//        System.out.println((char) fsDataInputStream.readByte());
//        System.out.println((char) fsDataInputStream.readByte());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(outf, 0L, fs.getFileStatus(outf).getLen());
        for (BlockLocation b : blockLocations){
            System.out.println(Arrays.toString(b.getStorageIds()));
            System.out.println(Arrays.toString(b.getCachedHosts()));
            System.out.println(Arrays.toString(b.getHosts()));
            System.out.println(Arrays.toString(b.getNames()));
            System.out.println(b.getOffset());
            System.out.println(b.getLength());
            System.out.println(Arrays.toString(b.getTopologyPaths()));
            System.out.println("------------------------------");
        }


        fs.close();

    }
}
