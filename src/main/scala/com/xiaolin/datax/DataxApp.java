package com.xiaolin.datax;

//import com.alibaba.datax.core.Engine;

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-12-04 13:52
 **/
public class DataxApp {
    public static void main(String[] args) {
        System.setProperty("datax.home", "D:\\IdeaProjects\\DataX\\target\\datax\\datax");
        String[] datxArgs = {"-job", "D:\\IdeaProjects\\spark-scala001\\datax_json\\mysql2mysql.json", "-mode", "local", "-jobid", "222"};

        try {
//            Engine.entry(datxArgs);
        }catch (Throwable e){
            e.printStackTrace();
        }
    }
}
