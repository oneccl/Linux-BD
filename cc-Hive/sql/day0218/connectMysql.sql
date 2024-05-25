show databases;

use day1122_ssm_sms;

select * from sms_user;

# 给Hive创建一个数据库
create schema metastore character set 'utf8';

use metastore;

show tables;



