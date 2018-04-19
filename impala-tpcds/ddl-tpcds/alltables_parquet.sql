drop database if exists ${VAR:DB} cascade;
create database ${VAR:DB};
use ${VAR:DB};

set parquet_file_size=512M;
set COMPRESSION_CODEC=snappy; 

drop table if exists call_center;
create table ${VAR:DB}.call_center
stored as parquet
as select * from ${VAR:HIVE_DB}.call_center;

drop table if exists catalog_page;
create table ${VAR:DB}.catalog_page
stored as parquet
as select * from ${VAR:HIVE_DB}.catalog_page;

drop table if exists catalog_returns;
create table ${VAR:DB}.catalog_returns
stored as parquet
as select * from ${VAR:HIVE_DB}.catalog_returns;

drop table if exists catalog_sales;
create table ${VAR:DB}.catalog_sales
stored as parquet
as select * from ${VAR:HIVE_DB}.catalog_sales;

drop table if exists customer_address;
create table ${VAR:DB}.customer_address
stored as parquet
as select * from ${VAR:HIVE_DB}.customer_address;

drop table if exists customer_demographics;
create table ${VAR:DB}.customer_demographics
stored as parquet
as select * from ${VAR:HIVE_DB}.customer_demographics;

drop table if exists customer;
create table ${VAR:DB}.customer
stored as parquet
as select * from ${VAR:HIVE_DB}.customer;

drop table if exists date_dim;
create table ${VAR:DB}.date_dim
stored as parquet
as select * from ${VAR:HIVE_DB}.date_dim;

drop table if exists household_demographics;
create table ${VAR:DB}.household_demographics
stored as parquet
as select * from ${VAR:HIVE_DB}.household_demographics;

drop table if exists income_band;
create table ${VAR:DB}.income_band
stored as parquet
as select * from ${VAR:HIVE_DB}.income_band;

drop table if exists inventory;
create table ${VAR:DB}.inventory
stored as parquet
as select * from ${VAR:HIVE_DB}.inventory;

drop table if exists item;
create table ${VAR:DB}.item
stored as parquet
as select * from ${VAR:HIVE_DB}.item;

drop table if exists promotion;
create table ${VAR:DB}.promotion
stored as parquet
as select * from ${VAR:HIVE_DB}.promotion;

drop table if exists reason;
create table ${VAR:DB}.reason
stored as parquet
as select * from ${VAR:HIVE_DB}.reason;

drop table if exists ship_mode;
create table ${VAR:DB}.ship_mode
stored as parquet
as select * from ${VAR:HIVE_DB}.ship_mode;

drop table if exists store_returns;
create table ${VAR:DB}.store_returns
stored as parquet
as select * from ${VAR:HIVE_DB}.store_returns;

drop table if exists store_sales;
create table ${VAR:DB}.store_sales
stored as parquet
as select * from ${VAR:HIVE_DB}.store_sales;

drop table if exists store;
create table ${VAR:DB}.store
stored as parquet
as select * from ${VAR:HIVE_DB}.store;

drop table if exists time_dim;
create table ${VAR:DB}.time_dim
stored as parquet
as select * from ${VAR:HIVE_DB}.time_dim;

drop table if exists warehouse;
create table ${VAR:DB}.warehouse
stored as parquet
as select * from ${VAR:HIVE_DB}.warehouse;

drop table if exists web_page;
create table ${VAR:DB}.web_page
stored as parquet
as select * from ${VAR:HIVE_DB}.web_page;

drop table if exists web_returns;
create table ${VAR:DB}.web_returns
stored as parquet
as select * from ${VAR:HIVE_DB}.web_returns;

drop table if exists web_sales;
create table ${VAR:DB}.web_sales
stored as parquet
as select * from ${VAR:HIVE_DB}.web_sales;

drop table if exists web_site;
create table ${VAR:DB}.web_site
stored as parquet
as select * from ${VAR:HIVE_DB}.web_site;

