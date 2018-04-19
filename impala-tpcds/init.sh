#!/bin/bash

#指定impala数据库名
db_name=$1
#源数据库名
source_db_name=$2
#impala daemon服务的hostname或ip地址
impala_daemon=$3


impala-shell -i $impala_daemon --var=DB=$db_name --var=HIVE_DB=$source_db_name -f ./ddl-tpcds/alltables_parquet.sql 

impala-shell -i $impala_daemon --var=DB=$db_name -f ./ddl-tpcds/analyze.sql
