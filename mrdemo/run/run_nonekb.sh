#!/bin/bash

JAVA_HOME=/usr/java/jdk1.8.0_131-cloudera

for file in `ls lib/*jar`
do
    CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH


${JAVA_HOME}/bin/java com.cloudera.mr.NodeKBMRTest
