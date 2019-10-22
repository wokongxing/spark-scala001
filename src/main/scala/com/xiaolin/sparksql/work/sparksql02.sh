#!/bin/sh
${SPARK_HOME}/bin/spark-submit --class com.xiaolin.sparksql.work.sparksql02 \
--jars hdfs://hadoop001:9000/lib/config-1.2.0.jar \
--driver-class-path /home/hadoop/lib/mysql-connector-java-5.1.47.jar \
--master local /home/hadoop/script/spark/spark-scala-1.0.jar
