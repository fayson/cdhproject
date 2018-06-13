#!/bin/bash
#########################################
# 创建Topic
# kafka-topics --create --zookeeper cdh01.fayson.com:2181,cdh02.fayson.com:2181,cdh03.fayson.com:2181 --replication-factor 3 --partitions 3 --topic kafka_sparkstreaming_hbase_topic
#
########################################

JAVA_HOME=/usr/java/jdk1.8.0_144

#要读取的文件
read_file=$1

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done
export CLASSPATH

${JAVA_HOME}/bin/java -Xms1024m -Xmx2048m com.cloudera.kerberos.ReadUserInfoFileToKafka $read_file
