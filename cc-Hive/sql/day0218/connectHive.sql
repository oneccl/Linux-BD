show databases;
show tables;
select * from stu;
// 显示表结构
desc stu;
desc extended stu;
// 格式化显示表结构的详细信息
desc formatted stu;

// Linux数据库数据导入导出
// 1）导入数据库数据：
-- 进入数据库sql命令框，选择使用的数据库
-- 执行sql脚本(.sql文件):
-- source .sql文件路径
// 2）导出数据库数据：
-- mysqldump -u root -p 123456 数据库名>文件名.sql

// 显示函数参数及说明
desc function split;
// 显示函数参数、说明、举例及其全类名
desc function extended split;

// 例: t_log表IP统计
select split(line," ")[0] ip from t_log limit 3;

select
t.ip,count(t.ip) count
from
(select split(line," ")[0] ip from t_log) t
group by t.ip
order by count desc
limit 10;

/* DDL（数据定义语言）*/

// 1、创建表
-- 语法：create [external] table [if not exits] 表名(列名1 类型 备注,...)
-- 1)内表（托管表）
create table if not exists stu1(
    name string comment '姓名',
    age int comment '年龄'
);
-- 2)external: 外表
create external table stu2(name string,age int);
-- 3)内表与外表区别：
-- ①表类型不同
desc formatted stu1; // Table Type: MANAGED_TABLE
desc formatted stu2; // Table Type: EXTERNAL_TABLE

// 2、删除表
// ②删除内容不同
drop table stu1; // 管理表：删除表结构和hdfs上的数据
drop table stu2; // 外表：只删除表结构，hdfs上的数据保留（保证数据可重用）
// 使用external外表保留数据：
// 若不知道字段数量，可使用(line string)，进行分割处理
create external table stu2_ext(name string,age int)
location 'hdfs://bd91:8020/user/hive/warehouse/stu2';
desc formatted stu2_ext; // hdfs://bd91:8020/user/hive/warehouse/stu2

// 3、显示表详情
desc stu1;
desc extended stu1;
desc formatted stu1;

// 4、截断表（truncate）
// 保留表结构，删除所有数据
// truncate 与 delete from 区别
truncate table stu2; // truncate：DDL；不支持where条件删除；对外表不支持
delete from stu2; // delete：DML；支持where条件删除；delete/update不支持

/* DML（数据操作(增删改)语言）*/

// 1、数据导入（添加数据）

-- 1）insert into
insert into stu1 values
("Jack",18),
("Tom",18);
insert into stu2 values
("aa",20);

-- 2）hdfs上传文件
// Hive本质上将表结构与hdfs文件一一映射，可以直接操作底层文件夹让Hive读取上传的文件
// hdfs dfs -put 文件 路径(hdfs://bd91:8020/user/hive/warehouse/表名)

-- 3）加载本地数据
-- 语法：load data [local] inpath 文件(可以是Linux本地路径或hdfs路径)
-- [overwrite] into table 表名;
// overwrite: 是否覆盖：省略不写(cp复制)；写(mv移动)
create external table l_log(line string);
load data local inpath '/opt/log-20211101.txt' into table l_log;

-- 4）as select语句：将后表查询结果写入前表中
create table s_stu1 as select * from stu1;

-- 5）其它见hivePartition.sql文件

// 2、delete/update（Hive不支持）
-- Hive底层将表的数据保存在hdfs，hdfs不支持文件的更新操作
-- 因此，Hive不支持表数据的delete和update操作

/* DQL（数据查询语言）*/

select * from t_log limit 3;
select * from s_stu1;
