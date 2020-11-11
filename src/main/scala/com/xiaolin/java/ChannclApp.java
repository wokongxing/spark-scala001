package com.xiaolin.java;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @program: spark-scala001 相对于 数据量大的时候,
 * @description:  MappedByteBuffer(映射内存) 速度 > allocateDirect(堆外内存)  > allocate (jvm内存)
 * @author: linzy
 * @create: 2020-10-27 10:07
 **/
public class ChannclApp {

    public static void main(String[] args) throws IOException {
//        File f = new File("D:\\IdeaProjects\\spark-scala001\\data\\dept.txt");
//        File f2 = new File("D:\\IdeaProjects\\spark-scala001\\outdata\\test.txt");
//        Byte[] b = new Byte[1024*1024*200];
//        ByteBuffer b1 = ByteBuffer.allocate(1024*1024*500);
//        //ByteBuffer b2 = ByteBuffer.allocateDirect(1024);
//        FileChannel in = new FileInputStream(f).getChannel();
////        FileChannel out = new FileOutputStream(f2).getChannel();
//          使用 MappedByteBuffer 映射内存
//        RandomAccessFile rf = new RandomAccessFile(f,"rws");
//        RandomAccessFile out = new RandomAccessFile(f2,"rws");
//        FileChannel channel = rf.getChannel();
//        FileChannel channelo = out.getChannel();
//        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
//
//        channelo.write(mappedByteBuffer);
//        channelo.close();
//        channel.close();

        System.out.println(1 << 29);
//        long start = System.currentTimeMillis();
//        in.read(b1);
//        long end = System.currentTimeMillis();
//        System.out.println("FileInputStream:读取 allocate ByteBuffer用时:"+(end - start));






    }
}
