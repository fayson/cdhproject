use iot_test;
create table if not exists impala_table_parquet (
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

insert overwrite table impala_table_parquet partition (subdir) select * from hive_table_parquet
