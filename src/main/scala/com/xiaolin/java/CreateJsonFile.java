package com.xiaolin.java;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @program: spark-scala001
 * @description: 生成一个json文件
 * @author: linzy
 * @create: 2020-10-29 16:07
 **/
public class CreateJsonFile {
    static  String json_demo ="{\"job\":{\"setting\":{\"speed\":{\"channel\":1},\"errorLimit\":{\"record\":0,\"percentage\":0.02}}}}";

    public static void main(String[] args) throws IOException {
        String rootpath="/data";
        String fliename="aj2gx";
        String filepath = rootpath+"/"+fliename+".json";

        File file = new File(filepath);

        if (file.getParentFile().exists()){
            file.getParentFile().mkdirs();
        }
        if (file.exists()){
            file.delete();
        }

        JSONObject jsonObject = new JSONObject();
        Object json = JSON.parse(json_demo);
        FileOutputStream outputStream = new FileOutputStream(file);
//        outputStream.write(json.toString().getBytes("utf-8"));
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.put(json.toString().getBytes("utf-8"));
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()){
            outputStream.getChannel().write(byteBuffer);
        }
        outputStream.getChannel().close();
        outputStream.close();
        outputStream.flush();

    }
}
