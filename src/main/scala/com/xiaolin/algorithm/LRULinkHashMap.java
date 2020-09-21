package com.xiaolin.algorithm;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @program: spark-scala001
 * @description: LRUs 算法
 * LRU（Least recently used，最近最少使用）算法根据数据的历史访问记录来进行淘汰最近最少使用的数据，
 * 其核心思想是“如果数据最近被访问过，那么将来被访问的几率也更高”。
 * @author: linzy
 * @create: 2020-09-15 16:09
 **/
public class LRULinkHashMap extends LinkedHashMap {
    private final int maxCacheSize;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * accessOrder标志位设为true以便开启按访问顺序排序的模式
     * @param maxcachesize
     */
    public LRULinkHashMap(int maxcachesize) {
        super(maxcachesize, DEFAULT_LOAD_FACTOR, true);
        maxCacheSize = maxcachesize;
    }

    @Override
    /**
     * 重写LinkedHashMap中的removeEldestEntry方法，当LRU中元素多余时，
     *  删除最不经常使用的元素
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {
        return size()>maxCacheSize;
    }

    public static void main(String[] args) {
        LRULinkHashMap map = new LRULinkHashMap(4);
        map.put("a","1a");
        map.put("b","2b");
        map.put("c","3c");
        map.put("d","4d");
        map.forEach((key,value) ->{
            System.out.println(key+":"+value);
        });

        System.out.println("-------------最新访问的---------------------");
        map.get("c");
        map.forEach((key,value) ->{
            System.out.println(key+":"+value);
        });

        System.out.println("----------------------------------");
        map.put("e","433");
        map.put("f","444");
        map.forEach((key,value) ->{
            System.out.println(key+":"+value);
        });
    }
}


