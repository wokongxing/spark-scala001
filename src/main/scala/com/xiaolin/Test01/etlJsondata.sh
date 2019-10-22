#!/bin/sh
./spark-submit \
    --class com.xiaolin.Test01.EtlJsonData \
    --master local \
    /home/hadoop/script/spark/spark-scala-1.0.jar

