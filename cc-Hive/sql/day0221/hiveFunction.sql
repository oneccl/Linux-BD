create database hiveFuncs;

use hiveFuncs;

// 元数据
create table meta_logDetail as
select
    d.arr[0] ip,
    d.arr[8] code,
    split(d.arr[3],":")[1] hour,
    d.arr[9] upload,
    d.arr[size(arr)-1] download
from
    (select split(line,"\\s+") arr from hivecrud.weblog) d;

select * from meta_logDetail;

// 1、Hive内置函数
// 官方文档:https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF

// Hive常用函数

create table user_demo(u_name string,u_sex int,u_phone string);
insert into user_demo values
("Tom",1,65536),
("Jack",0),
("Bob",1,50070);
select * from user_demo;

-- 1）空字段赋值: 为null时取默认值
-- nvl(v,default_v) 或 coalesce(v,default_v)
select u_name,nvl(u_phone,0) from user_demo;
select u_name,coalesce(u_phone,0) from user_demo;
-- 2）选择:
-- A、if(true?,true_v,false_v)
-- B、case when boolean表达式1 then 表达式1为True的取值
--        when boolean表达式2 then 表达式2为True的取值
--        ......
--        else 以上都为False的值 end
select u_name,if(u_sex==1,'男','女') from user_demo;

create table person_info(
    name string,
    constellation string,
    blood_type string)
row format delimited fields terminated by "\t";
load data local inpath "/opt/exercises/personInfo.txt" into table person_info;
select * from person_info;

-- 3）字符串连接与分割
-- 3.1、concat(s1,s2,...): 将多个字符串拼接
select concat("con","cat");  // concat
-- 3.2、concat_ws(分割符,s1,s2,...): 将多个字符串拼接并指定分割符
select concat_ws("|","con","cat");  // con|cat

-- 4）a.行转列: 将一列数据收集到集合中
-- 4.1、collect_set(col): 将指定字段col去重汇总，返回array类型字段
-- 4.2、collect_list(col): 将指定字段col不去重汇总，返回array类型字段
-- 4）b.列转行: 将array/map数据转为多行(列)
-- 4.3、explode(array): array中的每个元素生成一行
-- 4.4、explode(map): map中每个k-v生成一行，k为一列，v为一列
-- 列转行见rowFormat.sql文件

// 例：将星座和血型一样的人归类到一起
select
    concat_ws("&",constellation,blood_type),
    collect_list(name)
from person_info
group by constellation, blood_type;

create table business(
    name string,
    orderDate string,
    cost int
) row format delimited fields terminated by ',';
load data local inpath "/opt/exercises/business.txt" into table business;
select * from business;

-- 5）窗口函数（开窗函数）
-- over(): 指定分析函数工作的数据窗口大小
-- unbounded/n preceding: 向前无边界（从起点）/前n行
-- unbounded/n following: 向后无边界（到末尾）/后n行
-- current row: 到当前行
-- lag(col,n,default_v): 当前行获取列col往前第n个数据
-- lead(col,n,default_v): 当前行获取列col往后第n个数据
-- first_value(col): 当前窗口col列的第1个值
-- last_value(col): 当前窗口col列的最后1个值
-- ntile(n): 将当前窗口数据平均分成5份/组，每份/组都有唯一编号，编号从1开始

// 例1：查询在2017年4月份购买过的顾客及总人数
select * from business where orderDate like '2017-04%';
-- 指定函数count的工作窗口为：基于2017年4月数据并根据name分组后的窗口
select
name,count(1) over()
from business
where orderDate like '2017-04%'
group by name;
// 例2：查询顾客的购买明细及月购买总额
select
-- 指定函数sum的工作窗口为：基于根据name和month分区分区后的窗口统计
-- month(date): 获取月份
*,sum(cost) over(partition by name,month(orderDate)) month_cost
from business;
// 例3：查询每个顾客的cost总和
select name,sum(cost) from business group by name; -- 或
select distinct name,
sum(cost) over(partition by name)  -- 按name分组，组内(窗口内)cost进行sum
from business;
// 扩展（累加）rows必须跟在order by子句之后
select *,
-- 按name分组，组内排序累加
sum(cost) over(partition by name order by orderDate) t1,
-- 按name分组，由起点到当前行累加
sum(cost) over(partition by name order by orderDate rows between unbounded preceding and current row) t2,
-- 按name分组，当前行与前1行累加
sum(cost) over(partition by name order by orderDate rows between 1 preceding and current row) t3,
-- 按name分组，当前行到组内最后一行累加
sum(cost) over(partition by name order by orderDate rows between current row and unbounded following) t4,
-- 按name分组，当前行与前1行与后1行累加
sum(cost) over(partition by name order by orderDate rows between 1 preceding and 1 following) t5
from business;
// 例4：查询顾客购买明细以及上次的购买时间和下次购买时间
select *,
-- 获取orderDate列往前第1个值，若没有取默认值0000-00-00
lag(orderDate,1,"0000-00-00") over(partition by name order by orderDate) pre_orderDate,
-- 获取orderDate列往后第1个值，若没有取默认值0000-00-00
lead(orderDate,1,"0000-00-00") over(partition by name order by orderDate) next_orderDate
from business;
// 例5：查询顾客每个月第一次的购买时间和每个月的最后一次购买时间
select *,
-- 当前窗口orderDate列的第1个值
first_value(orderDate) over(partition by name,month(orderDate) order by orderDate) first_orderDate,
-- 当前窗口orderDate列的最后1个值
last_value(orderDate) over(partition by name,month(orderDate) order by orderDate rows between unbounded preceding and unbounded following) last_orderDate
from
business;
// 例6：查询前20%的订单信息
select * from (
-- ntile(5)：将当前窗口数据平均分成5份(编号1~5)，每份都有1个唯一编号，编号从1开始
select *,ntile(5) over(order by orderdate) sorted from business) t
-- 拿出编号为1的数据(1/5=20%)
where sorted = 1;

create table score(
    name string,
    subject string,
    score int)
row format delimited fields terminated by "\t";
load data local inpath '/opt/exercises/score.txt' into table score;
select * from score;

// 6）rank排序函数
-- rank(): 字段值相同时，序号会重复，总数不变；例：1 1 3 4 ...
-- dense_rank(): 字段值相同时，序号会重复，总数减少；例：1 1 2 3 ...
-- row_number(): 无论是否字段值相同，都从1开始计数；例：1 2 3 4 ...

// 例：计算每门学科成绩排名前3
select * from (
select *,
rank() over(partition by subject order by score desc) rank_order,
dense_rank() over(partition by subject order by score desc) dense_rank_order,
row_number() over(partition by subject order by score desc) row_num_order
from score) t
where rank_order<=3;

// 字符串函数
-- 1、regexp_extract(string s, string pattern, int index)：正则解析
-- 取字符串中匹配正则的第index个值
select regexp_extract("pattern200parse20",'[0-9]+',0); // 200
-- 2、parse_url(url,parse内容)
-- HOST,PATH,QUERY K
select parse_url('http://localhost:8080/ctl?k1=v1&k2=v2','QUERY','k1');
-- 3、JSON解析函数：见dataWarehouse.sql文件
-- 4、ascii(string str)：取首字符
select ascii("abc");  -- 97
select ascii("172.16.40.2")-48;  -- IP首字符（1的ASCII为49）
-- 5、length(string str)：获取字符串长度
select length('abc');  -- 3
-- 6、regexp_replace(string A, string pattern, string C)：替换字符串中符合正则的子串
select regexp_replace('abc10cd18e','[0-9]+','');  -- abcde
-- 7、substr(s,sta,end): 截取字符串(从1开始) [sta,end]
-- substr(s,-len): 截取最右len长度的子串
-- substring(s,sta,step): 截取字符串(从1开始) [sta,sta+step]
-- Hive没有提供left(str,len)、right(str,len)函数，可以使用substr()代替
-- 例：电话号码脱敏：
// select phone,concat(substr(phone,1,3),'****',substr(phone,-4)) from user;

-- Hive脱敏/屏蔽函数
-- mask(str): 默认将每个小写字母转换为x；将每个大写字母转换为X；将每个数字转换为n
-- mask_first_n(str,n): 对前n个字符进行屏蔽掩码处理
-- mask_last_n(str,n): 对后n个字符进行屏蔽掩码处理
-- mask_show_first_n(str,n): 对除了前n个的字符进行屏蔽掩码处理
-- mask_show_last_n(str,n): 对除了后n个的字符进行屏蔽掩码处理
-- mask_hash(str): 返回字符串的Hash编码

// 日期函数
-- 1、unix_timestamp()：获取当前系统时间戳
select unix_timestamp();
-- 2、from_unixtime()：时间戳格式化
select from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
-- 3、to_date()：获取日期时间字段中的日期部分
select to_date('2023-02-25 13:51:15');  -- 2023-02-25
-- 4、year、month、day、hour、minute、second: 获取日期时间字段中对应的部分
select month('2023-02-25 13:51:15');  -- 2
-- 5、其它日期函数：见dataWarehouse.sql文件
-- date_add('date',±n)：date所在的前n天或后n天
-- next_day('date','周1~7')：date所在周的下一个周1~7
-- date_format(date/timeStamp/string,format): 将args1日期按args2格式化输出
-- last_day(date): 返回该日期所在月的最后1天

// 小数取整
-- 1、四舍五入：round(double,保留位数)
select round(3.1415926,2);  -- 3.14
select round(1.5);  -- 2 四舍五入到个位
select round(255,-1);  -- 260 四舍五入到十位
-- 2、向下取整：floor(double)
select floor(2,78);  -- 2
-- 3、向上取整：ceil(double)/ceiling(double)
select ceil(1.41);  -- 2

use hivecrud;

// 2、自定义函数
// Hive所有函数都在:org.apache.hadoop.hive.ql.udf.UDF包中
// 1）添加依赖hive-exec和函数打包依赖
// 2）自定义类，继承UDF类
// 3）编写方法evaluate，方法名不能变，但允许重载
// 4）将模块打成jar上传到HDFS
// 5）在Hive中创建函数: temporary为临时函数
-- create [temporary] function
-- db_name.func_name
-- as '类的引用'
-- using jar 'hdfs-jar所在路径'

// 注册函数
create temporary function
getIPArea
as 'funcs.UdfIPToArea'
using jar 'hdfs://bd91:8020/jars/cc-Hive-1.0-SNAPSHOT.jar';

// 删除函数
drop function getIPArea;
// 函数信息
desc function extended getIPArea;

// 6）自定义函数使用

select ip,getIPArea(ip) area from s_log limit 1;
