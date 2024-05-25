use hiveCrud;
// ODS(原始数据层)
create table weblog(line string) location '/user/hive/warehouse/t_log';
select * from weblog limit 3;

// 2、表的分桶

// 准备元数据，通过查询结果创建新表存储
-- 语法：create table 表名 as select语句
create table logDetail as

-- DWD(数据详情层)数据清洗：字段提取
select
d.arr[0] ip,
d.arr[8] code,
split(d.arr[3],":")[1] hour,
coalesce(d.arr[9],0) upload,
-- size(数组)：获取数组大小/长度
coalesce(d.arr[size(arr)-1],0) download
from
(select split(line,"\\s+") arr from weblog) d;

// (1)创建分桶表/目录
-- 将表的数据按照某个字段分类存储到不同文件夹，并支持桶内排序
create table weblog_bkt(
ip       string,
code     string,
hour     string,
-- SQL支持string类型的数字类型自动转bigint/int
upload   bigint,
download bigint
)
-- a.按照字段指定分桶
clustered by (code)
-- b.在分桶时，指定字段排序
sorted by (upload desc)
-- c.分桶数
into 11 buckets;
// 底层MR会根据桶的个数，自动启动若干个ReduceTask(=分桶数)并行计算
// 分桶数=分桶字段种类数为理想状态
// 一个桶中可以存放多个Key数据，一个Key数据不会被拆分到多个桶中

// (2)分桶表的结构
desc formatted weblog_bkt;

// (3)将logDetail表的数据添加到weblog_bkt分桶表
insert into weblog_bkt select * from logDetail;

// (4)查询code=200的信息
select * from weblog_bkt where code="200";

// (5)统计每个code出现的次数排行
select code,count(code) count
from weblog_bkt
group by code
order by count desc;


