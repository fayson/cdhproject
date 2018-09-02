#!/bin/bash


mkdir -p ldif
mkdir -p keytab
#OpenLDAP信息
ldap_url="ldap://cdh01.fayson.com"
user_base="ou=People,dc=fayson,dc=com"
group_base="ou=Group,dc=fayson,dc=com"
super_admin="cn=Manager,dc=fayson,dc=com"
super_password="123456"

#kerberos信息
domain="FAYSON.COM"

#输出异常日志方法
function show_errmsg() {
  echo -e "\033[40;31m[ERROR] $1 \033[0m" 
}

#输出高亮日志方法
function show_highlight() {
  echo -e "\033[40;34m $1 \033[0m" 
  exit
}

#查找OpenLDAP用户是否已存在
exists_user(){ 
  result=`ldapsearch -H $ldap_url -b "uid=${1},${user_base}" -D "$super_admin" -w $super_password | grep result: |awk -F " " '{print $2}'`
  return $result
}
