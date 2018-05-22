use iot_test;
CREATE VIEW IF NOT EXISTS  impala_view AS SELECT ordercoldaily,smsusedflow,gprsusedflow,hours_add(statsdate,8) as statsdate,custid,groupbelong,provinceid,apn,subdir FROM hive_table_parquet;
