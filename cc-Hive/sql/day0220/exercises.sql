create database exercises;

use exercises;

// 数据准备

// student
create table student(
    s_id int,
    s_name string,
    s_birth date,
    s_sex string
)
row format delimited
fields terminated by ','
stored as textfile
location '/hive_exercises/student'
-- 方式1：创建表时去掉第1行
tblproperties("skip.header.line.count"="1");

-- 方式2：已创建好表并导入数据，去掉第1行
-- alter table 表名 set tblproperties('skip.header.line.count'='1');

select * from student;

// course
create table if not exists course (
    c_id int,
    c_name string,
    t_id int
)
row format delimited fields terminated by ','
stored as textfile
location '/hive_exercises/course'
tblproperties("skip.header.line.count"="1");

select * from course;

// teacher
create table if not exists teacher (
    t_id int,
    t_name string
)
row format delimited fields terminated by ','
stored as textfile
location '/hive_exercises/teacher'
tblproperties("skip.header.line.count"="1");

select * from teacher;

// score
create table if not exists score (
    s_id int,
    c_id int,
    s_score int
)
row format delimited fields terminated by ','
stored as textfile
location '/hive_exercises/score'
tblproperties("skip.header.line.count"="1");

select * from score;

// 多表联查
create table ssct as
select
    st.s_id stuId,
    st.s_name stuName,
    st.s_birth stuBirth,
    st.s_sex stuSex,
    sc.s_score score,
    c.c_id courseId,
    c.c_name courseName,
    t.t_name teaName
from
    student st,
    score sc,
    course c,
    teacher t
where
    st.s_id=sc.s_id and sc.c_id=c.c_id and c.t_id=t.t_id;

select * from ssct;

// 1、查询"01"课程比"02"课程成绩高的学生的信息及课程分数
-- with：声明需要使用的视图
with
    cs1 as (select * from ssct where courseId=1),
    cs2 as (select * from ssct where courseId=2)
select
    cs1.stuId,cs1.stuName,
    cs1.courseName course1,cs1.score score1,
    cs2.courseName course2,cs2.score score2
from cs1 join cs2 on cs1.stuId=cs2.stuId
where cs1.score>cs2.score;

// 2、查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
select stuId,stuName,count(courseId),sum(score) ss from ssct group by stuId,stuName;

// 3、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
with
    t1 as (select * from ssct where courseId=1),
    t2 as (select * from ssct where courseId=2)
select t1.stuId,t1.stuName,t1.stuSex,
    t1.courseId c1,t1.score s1,
    t2.courseId c2,t2.score s2
from
t1 join t2 on t1.stuName=t2.stuName;

// 4、查询没有学习全部课程的同学的信息



