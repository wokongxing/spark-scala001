package com.xiaolin.hive;


import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * @program: spark-scala001
 * @description: hive 语法校验
 * @author: linzy
 * @create: 2021-05-20 15:40
 **/
public class Hivevalidate {

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME", "root");
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris","thrift://hadoop001:9083");
        hiveConf.set("hive.metastore.warehouse.dir","hdfs://hadoop001:9000/hive/warehouse");
        String sql = "select * from area2";

        CliSessionState ss = new CliSessionState(hiveConf);
        SessionState.start(ss);

        ParseDriver pd = new ParseDriver();
        Context context = new Context(hiveConf);
        ASTNode astNode = pd.parse(sql, context);

//        Driver queryState = new Driver();
       BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(hiveConf, astNode);

        sem.analyze(astNode,context);
        sem.validate();
    }
}
