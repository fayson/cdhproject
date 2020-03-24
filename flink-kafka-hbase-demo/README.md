## Flink Demo 示例代码

### 一、FLink2HBaseSample 示例说明

Flink2HBaseSample 示例主要是通过实时读取Socket数据,使用CountWindow创建将数据批量的转换为List\<Put>，再通过自定义的Sink实时的写入Kerberos环境的HBase

- 创建HBase表并授予测试用户对表的操作权限
```shell script
[root@cdh1 ~]# kinit hbase/admin
Password for hbase/admin@PREST.COM: 
[root@cdh1 ~]# hbase shell                                                                                                                
hbase(main):001:0> grant 'cdhadmin','RWCXA'
Took 0.6101 seconds                                                                                                                   
hbase(main):002:0> create 'flink_hbase','info'
Created table flink_hbase
Took 1.5389 seconds                                                                                                                   
=> Hbase::Table - flink_hbase
hbase(main):004:0> scan 'flink_hbase'
ROW                                COLUMN+CELL                                                                                        
0 row(s)
Took 0.2107 seconds                                                                                                                   
hbase(main):005:0> 

```

- 在模拟Socket的服务器上安装nc命令，执行nc命令模拟一个Socket服务
```shell script
yum -y install nmap-ncat
nc -l -p 19000 -v -4
```

- 使用maven命令编译打包，这里是将所有的依赖编译打包为一个胖包。因为在Flink中默认没有与HBase集成，所以Flink的运行环境中不包含HBase的依赖包
```shell script
mvn clean package
```

- 在Flink集群的Gateway节点上执行如下命令向集群提交Flink作业
```shell script
flink run -m yarn-cluster -yn 3 -yjm 1024 -ytm 1024 \
-yD security.kerberos.login.keytab=/opt/cloudera/keytabs/cdhadmin.keytab \
-yD security.kerberos.login.principal=cdhadmin  \
--class com.fayson.Flink2HBaseSample flink-kafka-hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar \
--hostname 192.168.0.234 \
--port 19000
```


### 二、Kafka2HBaseSample 示例说明

Flink2HBaseSample 示例主要是通过实时读取非Kerberos环境Kafka数据,使用CountWindow创建将数据批量的转换为List\<Put>，再通过自定义的Sink实时的写入Kerberos环境的HBase

- 在Kafka集群创建一个测试用的Topic
```shell script
kafka-topics --create --zookeeper 192.168.0.221:2181,192.168.0.222:2181,192.168.0.223:2181 --replication-factor 3 --partitions 3 --topic test_topic

kafka-console-producer --broker-list 192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092 --topic test_topic

kafka-console-consumer --topic test_topic --from-beginning --bootstrap-server 192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092

```

- 创建HBase表并授予测试用户对表的操作权限
```shell script
[root@cdh1 ~]# kinit hbase/admin
Password for hbase/admin@PREST.COM: 
[root@cdh1 ~]# hbase shell                                                                                                                
hbase(main):001:0> grant 'cdhadmin','RWCXA'
Took 0.6101 seconds                                                                                                                   
hbase(main):002:0> create 'flink_hbase','info'
Created table flink_hbase
Took 1.5389 seconds                                                                                                                   
=> Hbase::Table - flink_hbase
hbase(main):004:0> scan 'flink_hbase'
ROW                                COLUMN+CELL                                                                                        
0 row(s)
Took 0.2107 seconds                                                                                                                   
hbase(main):005:0> 

```

- 在Flink集群的Gateway节点上执行如下命令向集群提交Flink作业
```shell script
flink run -m yarn-cluster -yn 3 -yjm 1024 -ytm 1024 \
-yD security.kerberos.login.keytab=/opt/cloudera/keytabs/cdhadmin.keytab \
-yD security.kerberos.login.principal=cdhadmin  \
--class com.fayson.Kafka2HBaseSample flink-kafka-hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
```