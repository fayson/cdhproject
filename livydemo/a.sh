#!/usr/bin/env bash


[root@ip-172-31-30-69 cloudera]# scp livy.tar.gz ip-172-31-21-83.ap-southeast-1.compute.internal:/opt/cloudera/


[root@ip-172-31-21-83 cloudera]# cd /opt/cloudera/
[root@ip-172-31-21-83 cloudera]# tar -zxvf livy.tar.gz


[root@ip-172-31-21-83 cloudera]# useradd livy -g hadoop
[root@ip-172-31-21-83 cloudera]# id livy


[root@ip-172-31-21-83 cloudera]# chown -R livy:hadoop livy
[root@ip-172-31-21-83 cloudera]# ll livy

[root@ip-172-31-21-83 cloudera]# mkdir /var/log/livy
[root@ip-172-31-21-83 cloudera]# mkdir /var/run/livy
[root@ip-172-31-21-83 cloudera]# chown livy:hadoop /var/log/livy
[root@ip-172-31-21-83 cloudera]# chown livy:hadoop /var/run/livy


[root@ip-172-31-21-83 conf]# scp livy-env.sh.template livy-env.sh
[root@ip-172-31-21-83 conf]# scp spark-blacklist.conf.template spark-blacklist.conf
[root@ip-172-31-21-83 conf]# scp livy.conf.template livy.conf
[root@ip-172-31-21-83 conf]# chown livy:hadoop livy.conf livy-env.sh spark-blacklist.conf



kadmin.local -q "addprinc -randkey livy/ip-172-31-21-83.ap-southeast-1.compute.internal@FAYSON.COM
kadmin.local -q "addprinc -randkey HTTP/ip-172-31-21-83.ap-southeast-1.compute.internal@FAYSON.COM
kadmin.local -q "xst -k /root/livy.service.keytab livy/ip-172-31-21-83.ap-southeast-1.compute.internal@FAYSON.COM
kadmin.local -q "xst -k /root/spnego.service.keytab HTTP/ip-172-31-21-83.ap-southeast-1.compute.internal@FAYSON.COM



livy.server.auth.type = kerberos
livy.server.auth.kerberos.keytab = /etc/security/keytabs/spnego.service.keytab
livy.server.auth.kerberos.principal = HTTP/ip-172-31-16-68.ap-southeast-1.compute.internal@FAYSON.COM
livy.server.launch.kerberos.keytab = /etc/security/keytabs/livy.service.keytab
livy.server.launch.kerberos.principal = livy/ip-172-31-16-68.ap-southeast-1.compute.internal@FAYSON.COM



[root@ip-172-31-21-83 keytabs]# cd /etc/security/keytabs/
[root@ip-172-31-21-83 keytabs]# chown livy:hadoop livy.service.keytab
[root@ip-172-31-21-83 keytabs]# chown livy:hadoop spnego.service.keytab
[root@ip-172-31-21-83 keytabs]# ll



[root@ip-172-31-16-68 ~]# scp livy.service.keytab spnego.service.keytab ip-172-31-21-83.ap-southeast-1.compute.internal:/etc/security/keytabs/


[root@ip-172-31-21-83 ~]# sudo -u livy /opt/cloudera/livy/bin/livy-server start

[root@ip-172-31-21-83 ~]# sudo -u livy /opt/cloudera/livy/bin/livy-server stop


[root@ip-172-31-21-83 keytabs]# kinit -kt livy.service.keytab livy/ip-172-31-21-83.ap-southeast-1.com



[root@ip-172-31-16-68 ~]# kadmin.local -q "xst -norandkey -k /root/fayson.keytab fayson@FAYSON.COM"



Client {
    com.sun.security.auth.module.Krb5LoginModule required
    storeKey=true
    useKeyTab=true
    debug=true
    keyTab="/Volumes/Transcend/keytab/fayson.keytab"
    principal="fayson@FAYSON.COM";
};