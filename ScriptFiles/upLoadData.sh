#!/bin/sh

num=3
path='/tmp/hive'
#create directory
sudo -u hdfs hdfs dfs -mkdir -p $path
sudo -u hdfs hdfs dfs -chmod 777 $path

#upload file
let i=1
while [ $i -le $num ];
do
  hdfs dfs -put data${i}.txt $path
  let i=i+1
done

#list file
hdfs dfs -ls $path
