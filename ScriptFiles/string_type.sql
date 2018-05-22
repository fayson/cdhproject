use iot_test;
create table hive_table_parquet2 (
ordercoldaily BIGINT, 
smsusedflow BIGINT, 
gprsusedflow BIGINT, 
statsdate STRING, 
custid STRING, 
groupbelong STRING, 
provinceid STRING, 
apn STRING ) 
PARTITIONED BY ( subdir STRING ) 
STORED AS PARQUET;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
insert overwrite table hive_table_parquet2 partition (subdir) select * from hive_table_test
