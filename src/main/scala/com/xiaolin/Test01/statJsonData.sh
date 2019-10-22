#!/bin/sh
./spark-submit \
    --class com.xiaolin.Test01.StatJsonData \
    --master local \
    --jars  hdfs://hadoop001:9000/lib/config-1.2.0.jar \
    --driver-class-path hdfs://hadoop001:9000/lib/mysql-connector-java-5.1.47.jar \
    /home/hadoop/script/spark/spark-scala-1.0.jar

