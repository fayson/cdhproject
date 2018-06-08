#!/bin/bash

JAVA_HOME=/usr/java/jdk1.8.0_131-cloudera

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH

${JAVA_HOME}/bin/java com.cloudera.hdfs.basic.HDFSAdminTest $@
[root@cdh01 hdfs-admin-run]# klist
[root@cdh01 hdfs-admin-run]# hadoop fs -ls /testquota
[root@cdh01 hdfs-admin-run]# hadoop fs -put run.sh /testquota
[root@cdh01 hdfs-admin-run]# hadoop fs -put /root/jdk-8u131-linux-x64.tar.gz /testquota


[root@cdh01 hdfs-admin-run]# sh run.sh setSpaceQuota /testquota 134217728


[root@cdh01 hdfs-admin-run]# klist
[root@cdh01 hdfs-admin-run]# hadoop fs -rmr /testquota/*
[root@cdh01 hdfs-admin-run]# hadoop fs -put /root/jdk-8u131-linux-x64.tar.gz /testquota


[root@cdh01 hdfs-admin-run]# sh run.sh clrAllQuota /testquota

[root@cdh01 hdfs-admin-run]# hadoop fs -put /root/jdk-8u131-linux-x64.tar.gz /testquota
[root@cdh01 hdfs-admin-run]# hadoop fs -put run.sh /testquota
[root@cdh01 hdfs-admin-run]# hadoop fs -ls /testquota