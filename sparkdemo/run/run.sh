#!/bin/bash

JAVA_HOME=/usr/java/jdk1.8.0_131-cloudera

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH


${JAVA_HOME}/bin/java com.cloudera.streaming.Test
export SPARK_DIST_CLASSPATH=$SPARK_DIST_CLASSPATH:/opt/cloudera/parcels/CDH/jars/spark-streaming-kafka_2.10-1.6.0-cdh5.12.1.jar