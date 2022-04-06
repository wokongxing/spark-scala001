package com.xiaolin.utils;

import org.lionsoul.ip2region.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;


public class  Ip2Util {
    private static DbSearcher searcher=null;
     private static  int algorithm = DbSearcher.BTREE_ALGORITHM; //B-tree

    public static void main(String[] args) {
        //中国|0|上海|上海市|有线通
        searcher=init();

        System.out.println(getAaddressByIp("121.76.22.101"));
        destory();
    }

    public static DbSearcher init() {
        //db
        String dbPath = Ip2Util.class.getResource("/ip2region.db").getPath();
        //String dbPath = "/home/hadoop/data/ip2region.db";

        File file = new File(dbPath);

        if ( file.exists() == false ) {
            System.out.println("Error: Invalid ip2region.db file");
        }

        //查询算法
        //DbSearcher.BINARY_ALGORITHM //Binary
        //DbSearcher.MEMORY_ALGORITYM //Memory

        DbConfig config = null;
        try {
            config = new DbConfig();
            return new DbSearcher(config, dbPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (DbMakerConfigException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String getAaddressByIp(String ip)   {

        try {
            //define the method
            Method method = null;
            switch ( algorithm )
            {
                case DbSearcher.BTREE_ALGORITHM:
                    method = searcher.getClass().getMethod("btreeSearch", String.class);
                    break;
                case DbSearcher.BINARY_ALGORITHM:
                    method = searcher.getClass().getMethod("binarySearch", String.class);
                    break;
                case DbSearcher.MEMORY_ALGORITYM:
                    method = searcher.getClass().getMethod("memorySearch", String.class);
                    break;
            }

            DataBlock dataBlock = null;
            if ( Util.isIpAddress(ip) == false ) {
                System.out.println("Error: Invalid ip address");
            }

            dataBlock  = (DataBlock) method.invoke(searcher, ip);

            return dataBlock.getRegion();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void destory(){
        try {
            searcher.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }


}
