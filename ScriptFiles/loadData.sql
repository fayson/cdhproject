use iot_test;
LOAD DATA INPATH '/tmp/hive/data1.txt' INTO TABLE hive_table_test partition (subdir="10");
LOAD DATA INPATH '/tmp/hive/data2.txt' INTO TABLE hive_table_test partition (subdir="20");
LOAD DATA INPATH '/tmp/hive/data3.txt' INTO TABLE hive_table_test partition (subdir="30");
