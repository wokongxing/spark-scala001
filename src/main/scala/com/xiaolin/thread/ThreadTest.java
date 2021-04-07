package com.xiaolin.thread;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-12-04 09:59
 **/
public class ThreadTest {
    public static void main(String[] args) {
//
//        Read read = new Read();
//        Write write = new Write();
//        new Thread(write).start();
//
//        new Thread(read).start();

        // 线程一
        new Thread(() -> {
            List<byte[]> list = new ArrayList<byte[]>();
            while (true) {
                System.out.println(new Date().toString() + Thread.currentThread() + "==");
                byte[] b = new byte[1024 * 1024 * 1];
                list.add(b);
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        // 线程二
        new Thread(() -> {
            while (true) {
                System.out.println(new Date().toString() + Thread.currentThread() + "==");
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }
}
