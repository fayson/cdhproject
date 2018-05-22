#!/bin/sh

#Concurrency test
let i=1
while [ $i -le $1 ];
do
 impala-shell -B -i cdh4.macro.com:25003 -u hive -f $2 -o log/${i}.out &
 let i=i+1
done
wait
