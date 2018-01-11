#!/bin/bash

JAVA_HOME=/usr/java/jdk1.8.0_131-cloudera

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH


${JAVA_HOME}/bin/java com.cloudera.streaming.Test


spark-submit --class com.cloudera.streaming.SparkSteamingHBase \
  --master yarn-client --num-executors 2 --driver-memory 1g \
  --driver-cores 1 --executor-memory 1g --executor-cores 1 \
  spark-demo-1.0-SNAPSHOT.jar