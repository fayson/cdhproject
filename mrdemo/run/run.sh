#!/bin/bash

JAVA_HOME=/usr/java/jdk1.8.0_131-cloudera

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

${JAVA_HOME}/bin/java com.cloudera.solr.TestKBSolr



[ec2-user@ip-172-31-22-86 ~]$ kdestroy
[ec2-user@ip-172-31-22-86 ~]$ kinit -kt fayson.keytab fayson
[ec2-user@ip-172-31-22-86 ~]$ klist
Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: fayson@CLOUDERA.COM

Valid starting       Expires              Service principal
12/06/2017 11:02:53  12/07/2017 11:02:53  krbtgt/CLOUDERA.COM@CLOUDERA.COM
        renew until 12/13/2017 11:02:53
[ec2-user@ip-172-31-22-86 ~]$

[ec2-user@ip-172-31-22-86 ~]$ hadoop jar mr-demo-1.0-SNAPSHOT.jar com.cloudera.mr.WordCount /fayson /wordcount/out