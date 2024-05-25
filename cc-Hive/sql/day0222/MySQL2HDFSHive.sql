

## 数据导入（MySQl-HDFS）

# 1）全表导入
/*
sqoop import \
--connect jdbc:mysql://node00:3306/hue_db \   数据库连接url
--username root \   用户名
--password 123456 \   密码
--table weblog_up_top10 \   表名
--target-dir hdfs://node01:8020/user/mysql2hdfs \   hdfs路径
--delete-target-dir \   如果路径存在则删除重建
--num-mappers 1 \   1个Map任务并行导入
--fields-terminated-by "\t"   按照分隔符
*/

# 2）查询导入
/*
sqoop import \
--connect jdbc:mysql://node00:3306/库名 \   数据库连接url
--username root \   用户名
--password 123456 \   密码
--target-dir hdfs://node01:8020/user/mysql2hdfs \   hdfs路径
--delete-target-dir \   如果路径存在则删除重建
--num-mappers 1 \   1个Map任务并行导入
--fields-terminated-by "\t" \   按照分隔符
--query 'select 字段 from 表名 where 条件 and $CONDITIONS;'  where子句中必须包含$CONDITIONS，若query后使用的是双引号，则$CONDITIONS前必须加转移符，防止shell识别为自己的变量
*/

# 3）部分导入：指定列和条件导入
/*
sqoop import \
--connect jdbc:mysql://node00:3306/库名 \   数据库连接url
--username root \   用户名
--password 123456 \   密码
--table 表名 \   表名
--columns 指定列(中间用,隔开) \  指定列
--where '筛选条件' \  筛选条件
--target-dir hdfs://node01:8020/user/mysql2hdfs \   hdfs路径
--delete-target-dir \   如果路径存在则删除重建
--num-mappers 1 \   1个Map任务并行导入
--fields-terminated-by "\t"   按照分隔符
*/

## 数据导入（MySQL-Hive）
# sqoop会将MySQL的数据先导入到HDFS临时目录(默认/user/用户名/表名)
# 然后再将数据加载到Hive中，加载完成后，会将临时存放的目录删除

/*
sqoop import \
--connect jdbc:mysql://node00:3306/hue_db \   数据库连接url
--username root \   用户名
--password 123456 \   密码
--table weblog_up_top10 \   表名
--num-mappers 1 \   1个Map任务并行导入
--hive-import \   Hive（导入）
--fields-terminated-by "\t" \   分隔符
--hive-overwrite \   覆写导入
--hive-table test.mysql2hive   指定Hive表（如果表不存在自动创建）
*/
# --hive-database 库名 指定Hive数据库

