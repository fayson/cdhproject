#!/bin/sh
username='hive'
password=' '
#echo $username,$password
#beeline -u jdbc:hive2://localhost:10000 $username $password
beeline -u jdbc:hive2://cdh2.macro.com:10000 $username $password
