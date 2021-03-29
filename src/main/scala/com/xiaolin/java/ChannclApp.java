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

    public static void main(String[] args) {
        File f = new File("D:\\IdeaProjects\\spark-scala001\\data\\dept.txt");
        File f2 = new File("D:\\IdeaProjects\\spark-scala001\\outdata\\test.txt");
        Byte[] b = new Byte[1024];
        ByteBuffer b1 = ByteBuffer.allocate(1024);
        //ByteBuffer b2 = ByteBuffer.allocateDirect(1024);
        FileChannel in =null;
        FileChannel out =null;
        try {
//             in  = new FileInputStream(f).getChannel();
//             out = new FileOutputStream(f2).getChannel();
//             in.transferTo(0,in.size(),out);
//            in.position(25L);
//            while (in.read(b1)!=-1){
//                b1.flip();
//                out.write(b1);
//                b1.clear();
//            }

//
//          使用 MappedByteBuffer 映射内存
        RandomAccessFile rf = new RandomAccessFile(f,"rws");
        RandomAccessFile out2 = new RandomAccessFile(f2,"rws");
        FileChannel channel = rf.getChannel();
        FileChannel channelo = out2.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
//
        channelo.write(mappedByteBuffer);

        out2.close();
        rf.close();
        channelo.close();
        channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (in !=null){
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out !=null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }





    }
}
