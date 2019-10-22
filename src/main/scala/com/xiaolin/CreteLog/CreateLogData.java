package com.xiaolin.CreteLog;

import com.alibaba.fastjson.JSON;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class CreateLogData {
    //本地测试路径
    private static String FLUME_LOCAL_URL="http://localhost:8085/flume/createlog";
    //通过nginx转发
    private static String FLUME_HADOOP001_URL="http://114.67.98.145:8089/flume/createlog";

    public static void main(String[] args) {
        CreateData();
    }

    public static void CreateData(){

        int size =10000;
        ArrayList<String> list = new ArrayList<String>(10000);
        //先设置固定的域名
        String domianNames[]={"www.baidu.com","www.ruozedata.com","www.alibaba.com"};
        String plat[]={"IOS","Android","Symbian","Window"};
        //错误数据
        String flows[]={"0","1","2","b","3","4","5","6","c","7","8","9","a"};
        StringBuffer stringBuffer;
        Random rdint = new Random();
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss Z");
        AccessLog accessLog = new AccessLog();
        for (int i=0;i<size;i++){
            //获取随机域名
            int index = rdint.nextInt(3);
            accessLog.setDomain(domianNames[index]);
            //stringBuffer.append("ruozedata.com").append("\t");
            //获取随机时间
            accessLog.setTime("["+sdf.format(new Date())+"]");
            //stringBuffer.append("[").append(sdf.format(new Date())).append("]").append("\t");
            //获取随机流量,制造随机错误数据
            int flow=rdint.nextInt(100000)+1000;
            accessLog.setFlow(flow+flows[rdint.nextInt(13)]);
            //stringBuffer.append(flow).append(flows[rdint.nextInt(13)]).append("\t");
            //获取随机IP
            accessLog.setIp(getRandomIp());
            //stringBuffer.append(getRandomIp()).append("\n");
            //list.add(stringBuffer.toString());
            //获取随机平台
            index = rdint.nextInt(4);
            accessLog.setPlat(plat[index]);
            sendPost(FLUME_LOCAL_URL, JSON.toJSONString(accessLog),"UTF-8");
        }
        //写入数据到本地
        //writeDataToLoacl(list);

    }
    //写入数据到本地
    public void writeDataToLoacl( ArrayList<String> list) {
        String dstpath="data/access02.log";
        FileOutputStream fileOutputStream=null;
        FileChannel fileChannel=null;
        ByteBuffer buffer =null;
        int size =list.size();
        try {
            fileOutputStream = new FileOutputStream(new File(dstpath));
            fileChannel=fileOutputStream.getChannel();

            for (int i=0;i<size;i++){
                buffer = ByteBuffer.wrap(list.get(i).toString().getBytes());
                fileChannel.write(buffer);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(fileOutputStream!=null){
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(fileChannel!=null){
                try {
                    fileChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
    public void getip(){
        String time ="[09/17/2019 17:23:55 +0800]";
        System.out.println(time.substring(1,time.length()-1));
    }

    /*
     * 随机生成国内IP地址
     */
    public static String getRandomIp() {

        // ip范围
        int[][] range = { { 607649792, 608174079 },// 36.56.0.0-36.63.255.255
                { 1038614528, 1039007743 },// 61.232.0.0-61.237.255.255
                { 1783627776, 1784676351 },// 106.80.0.0-106.95.255.255
                { 2035023872, 2035154943 },// 121.76.0.0-121.77.255.255
                { 2078801920, 2079064063 },// 123.232.0.0-123.235.255.255
                { -1950089216, -1948778497 },// 139.196.0.0-139.215.255.255
                { -1425539072, -1425014785 },// 171.8.0.0-171.15.255.255
                { -1236271104, -1235419137 },// 182.80.0.0-182.92.255.255
                { -770113536, -768606209 },// 210.25.0.0-210.47.255.255
                { -569376768, -564133889 }, // 222.16.0.0-222.95.255.255
        };

        Random rdint = new Random();
        int index = rdint.nextInt(10);
        String ip = numip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }

    /*
     * 将十进制转换成ip地址
     */
    public static String numip(int ip) {
        int[] b = new int[4];
        String x = "";

        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return x;
    }
    /**
     * 发送请求获得信息
     * @param uri
     * @return
     */
    private static String sendPost(String uri, String param, String charset) {
        String result = null;
        PrintWriter out = null;
        InputStream in = null;
        BufferedReader buffer =null;
        try {
            URL url = new URL(uri);
            HttpURLConnection urlcon = (HttpURLConnection) url.openConnection();
            urlcon.setRequestProperty("Content-type", "application/json");
            urlcon.setDoInput(true);
            urlcon.setDoOutput(true);
            urlcon.setUseCaches(false);
            urlcon.setRequestMethod("POST");
            urlcon.connect();// 获取连接
            out = new PrintWriter(urlcon.getOutputStream());
            out.print(param);
            out.flush();
            in = urlcon.getInputStream();
            buffer = new BufferedReader(new InputStreamReader(in, charset));
            StringBuffer bs = new StringBuffer();
            String line = null;
            while ((line = buffer.readLine()) != null) {
                bs.append(line);
            }
            result = bs.toString();
        } catch (Exception e) {
            System.out.println("[请求异常][地址：" + uri + "][参数：" + param + "][错误信息：" + e.getMessage() + "]");
        } finally {
            try {
                if (null != in)
                    in.close();
                if (null != out)
                    out.close();
            } catch (Exception e2) {
                System.out.println("[关闭流异常][错误信息：" + e2.getMessage() + "]");
            }
            if (buffer!=null){
                try {
                    buffer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}
