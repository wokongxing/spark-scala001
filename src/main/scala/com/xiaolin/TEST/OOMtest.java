package com.xiaolin.TEST;

import com.xiaolin.CreteLog.AccessLog;

/**
 * @auther 林
 * @Date 2019/11/7 11:14
 **/
public class OOMtest {

    public static void main(String[] args) {
        int i=1;
            for (;;){
                AccessLog log = new AccessLog();
                System.out.println(i);
                i++;
            }
    }
}
