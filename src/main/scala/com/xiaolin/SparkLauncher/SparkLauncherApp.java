package com.xiaolin.SparkLauncher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-09-14 17:25
 **/
public class SparkLauncherApp {
    private static String YARN_CONF_DIR = null;
    private static String HADOOP_CONF_DIR = null;
    private static String SPARK_HOME = null;
    private static String SPARK_PRINT_LAUNCH_COMMAND = "1";
    private static String Mater = null;
    private static String appResource = null;
    private static String mainClass = null;

    public static void main(String[] args) throws IOException, InterruptedException {

        if ("local".equals(args[0])){
            YARN_CONF_DIR="D:\\hadoop\\hadoop-2.6.0-cdh5.15.1\\etc\\hadoop";
            HADOOP_CONF_DIR="D:\\hadoop\\hadoop-2.6.0-cdh5.15.1\\etc\\hadoop";
            SPARK_HOME=System.getenv("SPARK_HOME");
            Mater = "local";
            appResource = "D:\\IdeaProjects\\spark-scala001\\target\\spark-scala-1.0.jar";
        } else {
            YARN_CONF_DIR="/home/hadoop/cluster/hadoop-release/etc/hadoop";
            HADOOP_CONF_DIR="/home/hadoop/cluster/hadoop-release/etc/hadoop";
            SPARK_HOME="/home/hadoop/cluster/spark-new";
            Mater = "yarn";
            appResource = "/home/hadoop/cluster/spark-new/examples/jars/spark-examples_2.11-2.2.1.jar";
        }
        HashMap env = new HashMap();
        env.put("HADOOP_CONF_DIR",HADOOP_CONF_DIR);
        env.put("YARN_CONF_DIR",YARN_CONF_DIR);
        SparkLauncher sparkLauncher = new SparkLauncher(env);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        sparkLauncher
                .setJavaHome(System.getenv("JAVA_HOME"))
                .setSparkHome(SPARK_HOME)
                .setAppName("test")
                .setAppResource(appResource)
                .setMaster(Mater)
                .setMainClass("main.scala.com.xiaolin.Test01.SparkTest")
                .startApplication(new SparkAppHandle.Listener(){

                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("state:" + handle.getState().toString());
                        System.out.println("appid:" + handle.getAppId());
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        System.out.println("Info:" + handle.getState().toString());
                    }
                });
        System.out.println("The task is executing, please wait ....");
        //线程等待任务结束
        countDownLatch.await();
        System.out.println("The task is finished!");
    }
}
