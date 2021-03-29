package com.xiaolin.thread;

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

        Read read = new Read();
        Write write = new Write();
        new Thread(write).start();

        new Thread(read).start();


    }
}
