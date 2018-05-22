use iot_test;
create table if not exists hive_table_parquet (
ordercoldaily BIGINT, 
smsusedflow BIGINT, 
gprsusedflow BIGINT, 
statsdate TIMESTAMP, 
custid STRING, 
groupbelong STRING, 
provinceid STRING, 
apn STRING ) 
PARTITIONED BY ( subdir STRING ) 
STORED AS PARQUET;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict; 
insert overwrite table hive_table_parquet partition (subdir) select * from hive_table_test
