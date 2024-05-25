use hiveCrud;

// 1、Hive数据类型

// 1）基本数据类型（Hive\Java对应）
-- tinyint(byte)、smallint(short)、int(int)、bigint(long)
-- boolean(boolean)、float(float)、double(double)、string(String)、date(Date)等
-- Hive的string相当于数据库的varchar，是一个可变长度字符串
// 2）集合类型
-- (1)array: 数组类型，访问：arr[index]；语法：array(e1,e2,...)
-- (2)map: 映射，键值对，访问：map['key']；语法：map(k1,v1,k2,v2,...)
-- (3)struct: 和对象类似，访问：字段名.元素

// 2、Hive创建表时自定义文件的行格式化

create table student(
name string,
score double,
hobby array<string>,
friends map<string,int>,
address struct<province:string,city:string,district:string>
)
-- 1）设置表存储数据的格式（行格式受限于）
row format delimited
-- 2）设置字段间的分割符（默认\u0001、\001）
fields terminated by ","
-- 3）设置集合(包括map)元素间的分隔符（默认\002）
collection items terminated by ";"
-- 4）设置map映射K-V间的分隔符（默认\003）
map keys terminated by ":"
-- 5）设置行结束的符号（行分隔符）(默认\n)
lines terminated by "\n"
-- 6）设置文件的存储格式（TextFile(默认)；Orc/Parquet:列式存储(压缩)，SequenceFile用于海量数据）
stored as textfile;

// 表结构
desc formatted student;

// 3、含复杂类型的表添加数据
-- 1）insert语法：insert into 表名 select ... ;
insert into student(name, score, hobby, friends) select
"李华",
88.5,
array('睡觉','学习','游戏'),
map('Tom',18,'Jack',17);

select * from student;

// 2）元素访问
select
hobby[0],friends["Tom"]
from student;

// 3）explode(array/map): 行转列
select explode(hobby) from student;
select explode(friends) from student;