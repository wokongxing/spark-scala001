package com.xiaolin.projectwork.one;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

/**
 * <p>
 * 1.消费kafka json数据转换为DF,然后show()
 */
public class PreWarningTest1 {

    //定义滑动间隔为5秒,窗口时间为30秒，即为计算每5秒的过去30秒的数据 最近30秒的数据指标
    // 30-35 35-40
    //         10--40
    private static final Duration slide_interval = new Duration(5 * 1000);
    private static final Duration window_length = new Duration(5 * 1000);


    private static final Pattern regexSpace = Pattern.compile(" ");

    static String[] spiltstr;
    static CDHRoleLog cdhRoleLog;
    static String sqlstr;
    static Timestamp recordTimestamp;
    static String key;
    static String value;
    static String host_service_logtype;

    private static JSONObject jsonlogline;

    private static void preWarning() {
        try {

            //1.使用 SparkSession,JavaSparkContext, JavaStreamingContext来定义 对象 jssc
            final SparkSession ss = new SparkSession.Builder()
                    .master("local[2]")
                    .appName("preWarning")
                    .getOrCreate();

            JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
            JavaStreamingContext jssc = new JavaStreamingContext(sc, slide_interval);



//            2.开启checkpoint机制，把checkpoint中的数据目录设置为hdfs目录
//            hdfs dfs -mkdir -p hdfs://nameservice1/spark/checkpointdata
//            hdfs dfs -chmod -R 777 hdfs://nameservice1/spark/checkpointdata
//            hdfs dfs -ls hdfs://nameservice1/spark/checkpointdata
            //jssc.checkpoint("hdfs://nameservice1/spark/checkpointdata");



            //3.设置kafka的map参数
            Map<String, Object> kafkaParams = new HashMap<String, Object>();
            kafkaParams.put("bootstrap.servers", "ruozedata001:9092,ruozedata002:9092,ruozedata003:9092"); //定义kakfa 服务的地址
            kafkaParams.put("key.deserializer", StringDeserializer.class);//key的序列化类
            kafkaParams.put("value.deserializer", StringDeserializer.class);//value的序列化类
            kafkaParams.put("group.id", "ruozedata");//制定consumer group
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);//是否自动确认offset

            //3.创建要从kafka去读取的topic的集合对象
            Collection<String> topics = Arrays.asList("PREWARNING");

            //4.输入流
            JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


            //5.将DS的RDD解析为JavaDStream<CDHRoleLog>     A DStream of RDD's that contain parsed CDH Role Logs.
            JavaDStream<CDHRoleLog> cdhRoleLogDStream =
                    lines.map(new Function<ConsumerRecord<String, String>, CDHRoleLog>() {
                        @Override
                        public CDHRoleLog call(ConsumerRecord<String, String> logline) throws Exception {
                            if (logline.value().contains("INFO") == true || logline.value().contains("WARN") == true || logline.value().contains("ERROR") == true || logline.value().contains("DEBUG") == true || logline.value().contains("FATAL") == true) {
                                try {
                                    //System.out.println(logline.value());
                                    //转换为json格式
                                    jsonlogline = JSONObject.parseObject(logline.value()); //只需要 flume采集的数据  不需要topic p offset等额外信息

                                    cdhRoleLog = new CDHRoleLog(jsonlogline.getString("hostname"),
                                            jsonlogline.getString("servicename"),
                                            jsonlogline.getString("time"),
                                            jsonlogline.getString("logtype"),
                                            jsonlogline.getString("loginfo"));
                                } catch (Exception ex) {

                                    System.out.println(ex.toString());
                                    cdhRoleLog = null;
                                }


                            } else {
                                //一个log的输出的非第一行，项目中暂时计划丢弃非第一行的数据
                                cdhRoleLog = null;
                            }

                            return cdhRoleLog;
                        }
                    });


            //6.过滤无效的RDD
            JavaDStream<CDHRoleLog> cdhRoleLogFilterDStream = cdhRoleLogDStream.filter(new Function<CDHRoleLog, Boolean>() {
                @Override
                public Boolean call(CDHRoleLog v1) throws Exception {
                    return v1 != null ? true : false;
                }
            });


            //7.Splits the cdhRoleLogFilterDStream into a dstream of time windowed rdd's.
            JavaDStream<CDHRoleLog> windowDStream =
                    cdhRoleLogFilterDStream.window(window_length, slide_interval);


            //8.使用foreachRDD
            windowDStream.foreachRDD(new VoidFunction<JavaRDD<CDHRoleLog>>() {
                @Override
                public void call(JavaRDD<CDHRoleLog> cdhRoleLogJavaRDD) throws Exception {

                    //8.1判断rdd的数目
                    if (cdhRoleLogJavaRDD.count() == 0) {
                        System.out.println("No cdh role logs in this time interval");
                        return;
                    }


                    // 8.2从RDD创建Dataset
                    Dataset<Row> cdhRoleLogDR = ss.createDataFrame(cdhRoleLogJavaRDD, CDHRoleLog.class);

                    cdhRoleLogDR.show(false);

                }
            });

            jssc.start(); //启动流式计算
            jssc.awaitTermination(); //等待中断
            jssc.close(); //关闭

        } catch (Exception e) {

            e.printStackTrace();

        }


    }


    public static void main(String[] args) {
        preWarning();
    }


}
