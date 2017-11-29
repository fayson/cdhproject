#!/bin/bash

for file in `ls lib/*jar` 
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH

for file in `ls /opt/cloudera/parcels/CDH/jars/*.jar`
do
   CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH

/usr/java/jdk1.8.0_131-cloudera/bin/java com.cloudera.solr.TestKBSolr
