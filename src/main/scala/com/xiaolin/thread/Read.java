package com.xiaolin.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-12-03 13:52
 **/
public class Read implements Runnable{

    private static final Logger LOG = LoggerFactory
            .getLogger(Read.class);

    private ArrayList buffer ;

    public void setBuffer(ArrayList buffer) {
        this.buffer = buffer;
    }

    @Override
    public void run() {

        try {
            LOG.debug("task reader starts to read ...");
            LocalTGStringManager.registerTaskGroupString(1,"22");
        } catch (Throwable e) {
            LOG.error("Reader runner Received Exceptions:", e);
        } finally {
            LOG.debug("task reader starts to do destroy ...");

        }
    }
}
