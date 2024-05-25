create database dataWarehouse;
use dataWarehouse;

// 数据仓库

// ODS（Opertional Data Store）: 原始数据层（log日志）

create table ods_logs(line string)
partitioned by (dt string);

load data local inpath '/opt/DataWarehouse/2019-06-17/logstart-.5'
into table ods_logs partition (dt='2019-06-17');

select * from ods_logs where dt='2019-06-11' limit 3;

// DWD（Data Warehouse Detail）: 数据清洗层（对null、脏数据等进行清洗）
// DIM（Dimension Table）: 数据维度层（将DWD层数据分成不同维度）

create table dwd_logs(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    lang string COMMENT '系统语言',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    email string COMMENT '邮箱',
    height_width string COMMENT '屏幕宽高',
    app_time string COMMENT '客户端日志产生时的时间',
    network string COMMENT '网络模式',
    entry string COMMENT '入口',
    open_ad_type string COMMENT '开屏广告类型',
    action string COMMENT '状态',
    loading_time string COMMENT '加载时长',
    detail string COMMENT '失败码',
    extend1 string COMMENT '扩展字段'
)
partitioned by (dt string);

-- JSON解析函数
-- 单层根据K获取V：get_json_object(json,'$.key')
-- 多层根据K获取V：get_json_object(json,'$.key1.key2....')
-- 根据K获取V(数组类型)值：get_json_object(json,'$.key[index]')
insert into dwd_logs partition (dt='2019-06-17')
select
    get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.eamil') email,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from ods_logs where dt='2019-06-17';

select * from dwd_logs where dt='2019-06-11' limit 50;

// DWM（Data Warehouse Middle）: 数据中间层（中间表，一般与DWS层合并）
// DWS/DWT（Data Warehoouse Service/Topic）: 数据业务层，通常与DWM层合并

// 日维度：日活跃用户
create table dwm_logs_day(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    lang string COMMENT '系统语言',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    email string COMMENT '邮箱',
    height_width string COMMENT '屏幕宽高',
    app_time string COMMENT '客户端日志产生时的时间',
    network string COMMENT '网络模式'
) COMMENT '日活跃用户'
partitioned by(dt string);

insert into dwm_logs_day partition (dt='2019-06-17')
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(email)) email,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network
from dwd_logs
where dt='2019-06-17'
group by mid_id;

select * from dwm_logs_day where dt='2019-06-11' limit 3;
select count(1) from dwm_logs_day where dt='2019-06-09';

// 周维度：周活跃用户
create table dwm_logs_wk(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    lang string COMMENT '系统语言',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    email string COMMENT '邮箱',
    height_width string COMMENT '屏幕宽高',
    app_time string COMMENT '客户端日志产生时的时间',
    network string COMMENT '网络模式',
    monday_date string COMMENT '周一日期',
    sunday_date string COMMENT  '周日日期'
) COMMENT '周活跃用户'
partitioned by (wk_dt string);

insert into dwm_logs_wk partition (wk_dt='2019-06-17~2019-06-23')
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(email)) email,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    -- next_day('date','周1~7')：date所在周的下一个周1~7
    -- date_add('date',±n)：date所在的前n天或后n天
    date_add(next_day('2019-06-17','MO'),-7), -- 上个周一
    date_add(next_day('2019-06-17','MO'),-1)  -- 上个周天
from dwm_logs_day
where dt>=date_add(next_day('2019-06-17','MO'),-7) and dt<=date_add(next_day('2019-06-17','MO'),-1)
group by mid_id;

select next_day('2023-02-22','MO');  // 该天所在的周一
select date_add('2023-02-22',1);  // 该天的后一天
select concat(date_add(next_day('2019-06-11','MO'),-7),'~',date_add(next_day('2019-06-11','MO'),-1));

select * from dwm_logs_wk where wk_dt='2019-06-10~2019-06-16' limit 5;
select count(1) from dwm_logs_wk where wk_dt='2019-06-10~2019-06-16';

// 月维度：月活跃用户
create table dwm_logs_mth(
    mid_id string COMMENT '设备唯一标识',
    user_id string COMMENT '用户标识',
    lang string COMMENT '系统语言',
    model string COMMENT '手机型号',
    brand string COMMENT '手机品牌',
    email string COMMENT '邮箱',
    height_width string COMMENT '屏幕宽高',
    app_time string COMMENT '客户端日志产生时的时间',
    network string COMMENT '网络模式'
) COMMENT '月活跃用户'
partitioned by (mth_dt string);

// 设置Hive动态分区
set hive.exec.dynamic.partition=true; -- 开启动态分区(默认false)
set hive.exec.dynamic.partition.mode=nonstrict; -- 允许动态分区(默认strict:不允许)
// 永久修改方式：修改Hive/conf/hive-default.xml.template中对应属性

// 动态分区使用
insert into dwm_logs_mth partition (mth_dt)
select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(email)) email,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    date_format('2019-06-09','yyyy-MM') mth_dt  -- 动态分区列
from dwm_logs_day
where date_format(dt,'yyyy-MM')=date_format('2019-06-09','yyyy-MM')
group by mid_id;

-- date_format(date/timeStamp/string,format): 将args1日期按args2格式化输出
select date_format("2023-02-23",'MM-dd');  -- 02-23

select * from dwm_logs_mth where mth_dt='2019-06' limit 10;
select count(1) from dwm_logs_mth; -- 990

// ADS（Application Data Store）: 数据应用层(BI)

// 统计dt当日活跃用户、dt当周活跃用户及dt当月活跃用户
create table ads_active_uc(
    dt string COMMENT '统计日期',
    day_count bigint COMMENT '当日用户数量',
    wk_count  bigint COMMENT '当周用户数量',
    mth_count  bigint COMMENT '当月用户数量',
    is_wkEnd string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    is_mthEnd string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃用户数'
row format delimited fields terminated by '\t';

// dt当日用户数量
select '2019-06-09' dt,count(1) day_auc from dwm_logs_day where dt='2019-06-09';
// dt当周用户数量
select '2019-06-09' dt,count(1) week_auc from dwm_logs_wk where
wk_dt=concat(date_add(next_day('2019-06-09','MO'),-7),'~',date_add(next_day('2019-06-09','MO'),-1));
// dt当月用户数量
select '2019-06-09' dt,count(1) mth_auc from dwm_logs_mth where
mth_dt=date_format('2019-06-09','yyyy-MM');

// 添加数据到表中
insert into ads_active_uc
-- 查询需求字段
select
    d.dt,        -- dt
    d.day_auc,   -- dt当日用户数量
    w.week_auc,  -- dt当周用户数量
    m.mth_auc,   -- dt当月用户数量
    if(date_add(next_day('2019-06-17','MO'),-1)=='2019-06-17','Y','N') isWkEnd, -- 是否是周末
    if(last_day('2019-06-17')=='2019-06-17','Y','N') isMthEnd -- 是否是月末
from
(select '2019-06-17' dt,count(1) day_auc from dwm_logs_day where dt='2019-06-17') d
join
(select '2019-06-17' dt,count(1) week_auc from dwm_logs_wk where
wk_dt=concat(date_add(next_day('2019-06-17','MO'),-7),'~',date_add(next_day('2019-06-17','MO'),-1))) w
on d.dt=w.dt join
(select '2019-06-17' dt,count(1) mth_auc from dwm_logs_mth where
mth_dt=date_format('2019-06-17','yyyy-MM')) m
on w.dt=m.dt;

-- last_day(date): 返回该日期所在月的最后1天
select last_day('2023-02-23');  -- 2023-02-28

select * from ads_active_uc;

// 将Hive表ads_active_uc数据导出到MySQL
-- 见Hive2MySQL.sql


