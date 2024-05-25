create database ipWarehouse;

use ipWarehouse;

create table bi_hour_usc(
    dt varchar(10),
    hour varchar(5),
    hourCount int,
    hourUpSum bigint,
    hourDownSum bigint
);

show tables;

select * from bi_hour_usc;

# 数据导入（Hive-MySQL）
/*
sqoop export \
--connect jdbc:mysql://bd91:3306/ipwarehouse?serverTimezone=Asia/Shanghai \
--username root \
--password 123456 \
--table bi_hour_usc \
--num-mappers 1 \
--export-dir hdfs://bd91:8020/user/hive/warehouse/ipwarehouse.db/dws_hour_usc \
--input-fields-terminated-by "," \
--columns dt,hour,hourCount,hourUpSum,hourDownSum
*/
