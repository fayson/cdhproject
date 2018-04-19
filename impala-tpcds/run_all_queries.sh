#!/bin/bash

impala_demon=ip-172-31-30-69.ap-southeast-1.compute.internal
database_name=tpcds_parquet_2

current_path=`pwd`
queries_dir=${current_path}/queries

rm -rf logs
mkdir logs

for t in `ls ${queries_dir}`
do
    echo "current query will be ${queries_dir}/${t}"
    impala-shell -B --database=$database_name -i $impala_demon -f ${queries_dir}/${t} &>logs/${t}.log
done
echo "all queries execution are finished, please check logs for the result!"
