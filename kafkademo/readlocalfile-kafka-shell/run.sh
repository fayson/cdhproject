#!/bin/bash
#########################################
# 创建Topic
# kafka-topics --create --zookeeper master.gzyh.com:2181,cdh01.gzyh.com:2181,cdh02.gzyh.com:2181 --replication-factor 3 --partitions 3 --topic ods_deal_daily_topic
#
########################################

JAVA_HOME=/usr/java/jdk1.8.0_131

#要读取的文件
read_file=$1

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH


${JAVA_HOME}/bin/java -Xms1024m -Xmx2048m com.cloudera.nokerberos.ReadFileToKafka $read_file