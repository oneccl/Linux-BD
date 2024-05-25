create database ipWarehouse;
use ipwarehouse;

select * from hivecrud.logdetail limit 5;

// 数据仓库

// ODS：原始数据层
-- 见hiveBucket.sql weblog表
create table ods_logs_ip(line string)
partitioned by (dt string);

load data local inpath '/opt/logs/log-20211102'
into table ods_logs_ip partition (dt='2021-11-02');

// DWD：数据详情/清洗层
-- 见hiveBucket.sql logDetail表
create table dwd_logs_ip(
    ip       string,
    code     string,
    hour     string,
    upload   bigint,
    download bigint
) partitioned by (dt string)
row format delimited fields terminated by ',';

insert into table dwd_logs_ip partition (dt='2021-11-02')
select
    d.arr[0],
    d.arr[8],
    split(d.arr[3],":")[1],
    coalesce(d.arr[9],0),
    coalesce(d.arr[size(arr)-1],0)
from
    (select split(line,"\\s+") arr from ods_logs_ip where dt='2021-11-02') d;

// DWM/DWS：数据中间层/业务层

-- 每个用户的数据
create table dws_usersInfo(
    ip string,
    code200 string,
    hours string,
    hourCounts string,
    upSum string,
    downSum string
)
partitioned by (dt string)
row format delimited fields terminated by ',';

insert into dws_usersInfo partition (dt='2021-11-02')
select    -- 再根据ip分组，得到每个用户的各个时间及对应访问次数
    ip,
    200 code200,
    concat_ws(',',collect_list(hour)) hours,
    concat_ws(',',collect_list(hourCount)) hourCounts,
    sum(upSum) upSum,
    sum(downSum) downSum
from
(select   -- 根据id和hour分组，得到每个用户每个时间的访问成功次数
    ip,
    hour,
    cast(count(hour) as string) hourCount,
    cast(sum(upload) as string) upSum,
    cast(sum(download) as string) downSum
from
dwd_logs_ip
where dt='2021-11-02' and code=='200'
group by ip,hour) t
group by t.ip;

-- implode()  MySQL把数组转换成字符串
-- explode()  MySQL把字符串转换成数组

select * from dws_usersInfo where dt='2021-11-01' limit 10;

-- 每小时用户访问量及上传下载流量
create table dws_hour_usc(
    dt string,
    hour string,
    hourCount int,
    hourUpSum bigint,
    hourDownSum bigint
)row format delimited fields terminated by ',';

insert into dws_hour_usc
select
    '2021-11-02' dt,
    hour,
    count(ip) hourCount,
    sum(upload) hourUpSum,
    sum(download) hourDownSum
from
dwd_logs_ip
where dt='2021-11-02' and code=='200'
group by hour;

select * from dws_hour_usc;
select * from dws_hour_usc where dt='2021-11-02';

desc formatted dws_hour_usc;

// ADS：数据应用层

// 将Hive表dws_hour_usc数据导出到MySQL
-- 见Hive2MySQL_IP.sql

