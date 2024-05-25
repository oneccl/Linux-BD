show databases;
create database hiveCrud;

use default;

// 例：统计ip,ipCount,code200,upload
select split(line,"\\s+") from t_log;
-- 创建指定数据库的表，保存当前数据库表的查询结果
-- create table hiveCrud.s_log as
select
lt.ip ip,
count(lt.ip) count,
sum(lt.code200) code200,
-- coalesce(表达式,表达式为Null时返回的值)
coalesce(sum(lt.upload),0) upload
from (
select
d.arr[0] ip,
-- if条件判断：if(boolean表达式,true结果,false结果)
if(d.arr[8]=='200',1,0) code200,
-- 类型转换：cast(原类型 as 目标类型)
cast(d.arr[9] as bigint) upload
from
(select split(line,"\\s+") arr from t_log) d) lt
group by ip
order by upload desc;

select * from s_log;

use hiveCrud;

// 1、表的分区

-- 1）创建分区表/目录，可以将数据按照不同分区分散存储
-- 粒度：1层目录
create table t_log_par(line string)
partitioned by (dt date); // 创建表的分区字段，日期类型的分区
// (1)表结构
desc formatted t_log_par;
// (2)数据导入（添加数据）
-- a.添加数据到指定分区，按照日期存储
insert into t_log_par partition (dt="2021-10-12") values("partition");
-- b.添加数据到指定分区，按照日期存储，并覆盖原来的分区
insert overwrite table t_log_par partition (dt="2021-10-12") values("abc");
-- c.加载集群数据到指定分区
load data inpath "/p.txt" into table t_log_par partition (dt="2021-10-14");
-- d.将查询结果(覆盖)添加到表中，可以指定分区添加
-- 语法：insert into 表名 [partition (分区)] select语句;
-- 语法：insert overwrite table 表名 [partition (分区)] select语句;
select * from t_log_par;

-- (3)分区字段具有与表字段相同功能的条件查询
-- 指定分区后，查询可以直接加载表中子目录的数据，避免加载全部数据
// 根据分区查询:
-- 查询2021-10-12的信息
select * from t_log_par where dt="2021-10-12";
-- (4)注：若通过hdfs创建分区目录，并上传文件，由于metastore中没有分区信息
-- 可能导致分区数据无法读取，需要修复/扫描分区信息
-- 语法：msck repair table 表名;

-- 2）分区表支持嵌套多级分区
-- 粒度：3层目录
create table t_log_par1(line string)
partitioned by (yy string,mm string,dd string);
// (1)分区表的结构
desc formatted t_log_par1;
// (2)加载本地数据到指定分区
load data local inpath "/opt/log-20211101.txt" into table t_log_par1
partition (yy="2021",mm="11",dd="1");
// (3)根据分区查询:
// a.查询2021年11月的信息
select * from t_log_par1 where yy="2021" and mm="11";
// b.查询每月1号的信息
select * from t_log_par1 where dd="1";




