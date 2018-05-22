create database if not exists iot_test;
use iot_test;
create table if not exists hive_table_test (
ordercoldaily BIGINT, 
smsusedflow BIGINT, 
gprsusedflow BIGINT, 
statsdate TIMESTAMP, 
custid STRING, 
groupbelong STRING, 
provinceid STRING, 
apn STRING ) 
PARTITIONED BY ( subdir STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," ;
