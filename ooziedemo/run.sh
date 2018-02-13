#!/bin/bash

name=$1

echo "hello $name" >> /tmp/oozieshell.log

sudo -u faysontest hadoop fs -mkdir -p /faysontest/jars
sudo -u faysontest hadoop fs -put /opt/cloudera/parcels/CDH/jars/spark-examples-1.6.0-cdh5.13.1-hadoop2.6.0-cdh5.13.1.jar /faysontest/jars
sudo -u faysontest hadoop fs -ls /faysontest/jars



[root@ip-172-31-6-148 ~]# sudo -u faysontest hadoop fs -mkdir -p /user/faysontest/oozie/testoozie
[root@ip-172-31-6-148 ~]# ll /opt/workflow.xml
-rwxr-xr-x 1 root root 810 Feb 13 12:23 /opt/workflow.xml
[root@ip-172-31-6-148 ~]# sudo -u hdfs hadoop fs -put /opt/workflow.xml /user/faysontest/oozie/testoozie
[root@ip-172-31-6-148 ~]# sudo -u hdfs hadoop fs -ls /user/faysontest/oozie/testoozie



sudo -u faysontest hadoop fs -mkdir -p /faysontest/jars
sudo -u faysontest hadoop fs -put /opt/cloudera/parcels/CDH/jars/hadoop-mapreduce-examples-2.6.0-cdh5.13.1.jar /faysontest/jars
sudo -u faysontest hadoop fs -ls /faysontest/jars


[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -mkdir -p /user/faysontest/oozie/javaaction
[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -put /opt/workflow.xml /user/faysontest/oozie/javaaction
[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -ls /user/faysontest/oozie/javaaction


sudo -u faysontest hadoop fs -mkdir -p /faysontest/jars
sudo -u faysontest hadoop fs -put /opt/ooziejob.sh /faysontest/jars
sudo -u faysontest hadoop fs -ls /faysontest/jars

[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -mkdir -p /user/faysontest/oozie/sehllaction
[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -put /opt/workflow.xml /user/faysontest/oozie/sehllaction
[root@ip-172-31-6-148 opt]# sudo -u faysontest hadoop fs -ls /user/faysontest/oozie/sehllaction

