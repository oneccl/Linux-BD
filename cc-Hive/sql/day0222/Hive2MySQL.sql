show databases;

create database hiveWarehouse;

use hivewarehouse;

create table bi_active_uc(
    dt varchar(10),
    day bigint,
    week bigint,
    month bigint,
    isWeekend varchar(2),
    isMonthEnd varchar(2)
) character set utf8;

select * from bi_active_uc;

# sqoop环境配置
# ETL（Extract-Transform-Load，即数据抽取、转换、装载）sqoop: ETL工具
/*
1、上传sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz到Linux，解压，修改名为sqoop-1.4.7_hadoop
2、将mysql-connector-java-8.0.25.jar放到sqoop-1.4.7_hadoop/lib/下
3、配置sqoop-1.4.7_hadoop/conf/sqoop-env.sh
  export HADOOP_COMMON_HOME=/opt/module/hadoop-2.7.7
  export HADOOP_COMMON_HOME=/opt/module/hadoop-2.7.7
  export HIVE_HOME=/opt/module/hive-2.3.9
  export JDK_HOME=/opt/module/jdk1.8
4、配置sqoop环境变量(/etc/profile)，刷新
*/

# 注意事项：
# 1）当Map任务数>1时，需要结合--split-by参数，指定分割键，以确定每个Map任务
# 到底读取哪一部分数据，最好指定数值型的列或主键（或分布均匀的列，避免数据倾斜）
# 2）sqoop在读取MySQL数据时，用的是JDBC方式，当数据量较大时，效率不是很高
# 3）sqoop底层通过MapReduce完成数据导入导出，只需要Map任务，不需要Reduce任务

-- Hive/Hdfs-MySQL
/*
bin/sqoop export \        Hive导出
--connect jdbc:mysql://bd91:3306/hivewarehouse \  连接数据库
--username root \         mysql用户名
--password 123456 \       mysql密码
--table bi_active_uc \    导入mysql中的表名
--num-mappers 1 \         MapTask数（默认4）
--export-dir hdfs://bd91:8020/user/hive/warehouse/datawarehouse.db/ads_active_uc \   hive表的路径
--input-fields-terminated-by "\t"  Hive表分隔符
--columns mysql表字段（若不写顺序出错，会导致解析错误）
*/

-- 执行语句
/*
bin/sqoop export
--connect jdbc:mysql://bd91:3306/hivewarehouse?serverTimezone=Asia/Shanghai
--username root
--password 123456
--table bi_active_uc
--num-mappers 1
--export-dir hdfs://bd91:8020/user/hive/warehouse/datawarehouse.db/ads_active_uc
--input-fields-terminated-by "\t"
--columns dt,day,week,month,isWeekend,isMonthEnd
*/

