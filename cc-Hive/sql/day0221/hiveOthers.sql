use hiveCrud;

// 1、数据导出
-- 1）insert：将查询结果导出到hdfs或本地(local)
-- 语法：insert overwrite [local] directory '路径(可自动创建递归目录)'
-- [row format delimited fields terminated by ',']
-- select语句;
insert overwrite directory '/HiveOut/s_log_out'
row format delimited
fields terminated by ','
select * from s_log;

-- 2）hdfs下载文件
-- hdfs dfs -get 文件路径(hdfs://bd91:8020/user/hive/warehouse/表名/) 本地路径

-- 3）Hive自带导出工具
-- export: 导出到hdfs
-- 语法：export table 表名(已存在) [partition(分区)] to hdfs路径;
export table hivecrud.weblog to '/HiveOut/weblog_out';

// 2、数据导入补充
-- 5）Hive自带导入工具
-- import: 从hdfs路径导入
-- 语法：import table 表名(不存在自动创建) [partition(分区)] from hdfs路径;
import table hiveCrud.import_wl from '/HiveOut/weblog_out';

select * from import_wl;

// 3、DQL（数据查询语言）
/*
select
  [distinct] 字段
from
  表名
[ join
    表名
  on
    连接条件 ]
where
  过滤条件
group by / distribute by
  分组条件: MR底层指定相应分区数 / 分区条件
having
  group by分组后过滤条件
order by / sort by
  排序条件: 全局排序，MR要实现全局排序，就只能启动1个ReduceTask / 局部排序，分区内排序
limit
  分页条件
*/

// 1）列举所有code
-- distinct 字段：字段去重
select distinct code from logdetail
-- str regexp '正则'：正则匹配
where substr(code,1,1) regexp '[0-9]';

// 2）查询IP以1开头的所有字段信息，并按上传流量排序，显示前100条
select
*
from
logdetail
-- substr(s,sta,end): 截取字符串(从1开始) [sta,end]
-- ip以1开头
where substr(ip,1,1)=='1'
order by cast(upload as bigint) desc
limit 100;

// 3）查询每种code码上传总流量大于1000的前3种code数量
select
code,count(code) codeCount,sum(upload) sumUpload
from
logdetail
-- 使用group by的select后面的字段有2个要求：有分组字段；有聚合函数
group by code
-- having只能用于group by分组统计后的过滤
having sum(upload)>1000
limit 3;

// 4、Hive-MR参数设置

// (1)Map分片数量设置：
// computeSplitSize(Math.max(minSize,Math.min(maxSize,blockSize)))=blockSize=128M
-- 设置单个切片的最大值（100M）
set mapreduce.input.fileinputformat.split.maxsize=100;
-- Map执行前合并小文件
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

// (2)ReduceTask任务数设置：
set mapreduce.job.reduces=3; -- 或
set mapred.reduce.tasks=3;

// 4）自定义MR分区，分区内排序
select
ip,upload
from
logdetail
-- MR分区(自定义Partitioner)，一般结合sort by使用
distribute by substr(ip,1,1)
-- 局部排序，分区内排序(减少shuffle)，合并输出
sort by cast(upload as bigint) desc;

-- 5）如果MR分区和排序是同一个字段，可以直接使用cluster by
-- cluster by = distribute by + sort by
select
ip,code
from
logdetail
where substr(code,1,1) regexp '[0-9]'
cluster by code;
