package com.xiaolin.thread;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-12-03 13:38
 **/
public class Write implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(Write.class);

    ReentrantLock lock = new ReentrantLock();
    Condition notEmpty = lock.newCondition();

    private ArrayList buffer ;

    public void setBuffer(ArrayList buffer) {
        this.buffer = buffer;
    }

    @Override
    public void run() {

        try {
            LOG.info("Writer Runner doing");

            lock.lockInterruptibly();
            while (true){
                LocalTGStringManager.updateTaskGroupString(1,"writer");
                System.out.println(Thread.currentThread().getId()+"---"+Thread.currentThread().getName());
                notEmpty.await(200L, TimeUnit.MILLISECONDS);
            }
        } catch (Throwable e) {
            LOG.error("Writer Runner Received Exceptions:", e);
        } finally {
            lock.unlock();
            LOG.debug("task writer starts to do destroy ...");
        }
    }
}
