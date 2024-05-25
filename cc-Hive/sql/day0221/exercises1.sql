use exercises;

show tables;

set hive.strict.checks.cartesian.product=flase;  -- 允许笛卡尔积操作
set hive.mapred.mode=nonstrict;  -- 非严格模式


-- 01.查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
with
    t1 as (select s_id from score where c_id = 1),
    t2 as (select s_id from score where c_id = 2),
    t3 as (select t1.s_id as s_id from t1 left join t2 on t1.s_id = t2.s_id where t2.s_id is null)
select *
from student s
where exists(select * from t3 where s.s_id = t3.s_id);

-- 02.查询没有学全所有课程的同学的信息
with
    t1 as (select count(*) as cnt_course from course),
    t2 as (select s_id,count(*) as cnt_course from score group by s_id),
    t3 as (select s_id from t1 join t2 where t1.cnt_course = t2.cnt_course)
select *
from student s
where not exists(select * from t3 where s.s_id = t3.s_id);

-- 03.查询至少有一门课与学号为"01"的同学所学相同的同学的信息
with
    t1 as (select c_id from score where s_id = 1),
    t2 as (select distinct s_id from score sc inner join t1 on sc.c_id = t1.c_id)
select *
from student s
where exists(select * from t2 where s.s_id = t2.s_id);

-- 04.查询和"01"号的同学学习的课程完全相同的其他同学的信息
with
    t1 as (select c_id from score where s_id = 1),
    t2 as (select count(*) as cnt_course from t1),
    t3 as (select s_id,count(*) as cnt_course from score sc
    inner join t1 on sc.c_id = t1.c_id where s_id != 1 group by s_id),
    t4 as (select s_id from t2 cross join t3 where t2.cnt_course = t3.cnt_course)
select *
from student s
where exists(select * from t4 where s.s_id = t4.s_id);

-- 05.查询各科成绩最高分、最低分和平均分，以如下形式显示：
-- 课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率
-- 及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
select
    c.c_id                                                                                 as course_id,
    c.c_name                                                                               as course_name,
    max(s_score)                                                                           as max_score,
    min(s_score)                                                                           as ming_score,
    -- 四舍五入：round(double,小数位数)
    round(avg(s_score), 2)                                                                 as avg_score,
    concat(round(sum(if(s_score >= 60, 1, 0)) / count(*) * 100, 2), '%')                   as pass_rate,
    concat(round(sum(if(s_score between 70 and 80, 1, 0)) / count(*) * 100, 2), '%')       as medium_rate,
    concat(round(sum(if(s_score between 80 and 90, 1, 0)) / count(*) * 100, 2), '%')       as good_rate,
    concat(round(sum(if(s_score >= 90, 1, 0)) / count(*) * 100, 2), '%')                   as excellent_rate
from course c
inner join score s on c.c_id = s.c_id
group by c.c_id,c.c_name;

-- 06.按各科成绩进行排序，并显示排名
select
    c.c_id,c.c_name,s.s_id,s.s_name,s_score,
    row_number() over(partition by c.c_id order by s_score desc) as rank
from score sc
inner join student s on sc.s_id = s.s_id
inner join course c on sc.c_id = c.c_id;

-- 07.查询学生的总成绩并进行排名
with
    t1 as (select s_id,sum(s_score) as sum_score from score group by s_id)
select s.s_id,s.s_name,sum_score,row_number() over(order by sum_score desc) as rank
from student s
inner join t1 on s.s_id = t1.s_id;

-- 08.查询不同老师所教不同课程平均分从高到低显示
select t.t_id,t.t_name,c.c_id,c.c_name,round(avg(s_score), 2) as avg_score
from score sc
inner join course c on sc.c_id = c.c_id
inner join teacher t on t.t_id = c.t_id
group by t.t_id,t.t_name,c.c_id,c.c_name
order by t.t_id,avg_score desc;

-- 09.查询所有课程的成绩第2名到第3名的学生信息及该课程成绩
with
    t1 as (
    select
    c.c_id,c.c_name,s.*,s_score,row_number() over(partition by c.c_id order by s_score desc) as rank
    from score sc
    inner join student s on sc.s_id = s.s_id
    inner join course c on sc.c_id = c.c_id
    )
select * from t1 where rank in (2,3);

-- 10.统计各科成绩各分数段人数：课程编号,课程名称,[100-85],[85-70],[70-60],[0-60]及所占百分比
select
    c.c_id                                                                            as course_id,
    c.c_name                                                                          as course_name,
    sum(if(s_score between 85 and 100, 1, 0))                                         as number_100_85,
    concat(round(sum(if(s_score between 85 and 100, 1, 0)) / count(*) * 100, 2), '%') as percentage,
    sum(if(s_score between 70 and 85, 1, 0))                                          as number_85_70,
    concat(round(sum(if(s_score between 70 and 85, 1, 0)) / count(*) * 100, 2), '%')  as percentage,
    sum(if(s_score between 60 and 70, 1, 0))                                          as number_70_60,
    concat(round(sum(if(s_score between 60 and 70, 1, 0)) / count(*) * 100, 2), '%')  as percentage,
    sum(if(s_score between 0 and 60, 1, 0))                                           as number_0_60,
    concat(round(sum(if(s_score between 0 and 60, 1, 0)) / count(*) * 100, 2), '%')   as percentage
from course c
inner join score sc
on c.c_id = sc.c_id
group by c.c_id,c.c_name;

-- 11.查询各科成绩前三名的记录
with
    t1 as (
    select
    c.c_id,c.c_name,s.s_id,s.s_name,s_score,
    row_number() over(partition by c.c_id order by s_score desc) as rank
    from score sc
    inner join student s on sc.s_id = s.s_id
    inner join course c on sc.c_id = c.c_id
    )
select * from t1 where rank <= 3;

-- 12.查询每门课程被选修的学生数
with
    t1 as (select c_id,count(*) as cnt_student from score group by c_id)
select c.c_id,c.c_name,cnt_student
from course c
inner join t1 on c.c_id = t1.c_id;

-- 13.查询出只有两门课程的全部学生的学号和姓名
with
    t1 as (select s_id from score group by s_id having count(*) = 2)
select *
from student s
where exists(select * from t1 where s.s_id = t1.s_id);

-- 14.查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编号升序排列
with
    t1 as (select c_id,round(avg(s_score), 2) as avg_score from score group by c_id)
select c.c_id,c.c_name,avg_score
from course c
inner join t1 on c.c_id = t1.c_id
order by avg_score desc,c.c_id;

-- 15.查询平均成绩大于等于85的所有学生的学号、姓名和平均成绩
with
    t1 as (select s_id,avg(s_score) as avg_score from score group by s_id having avg_score >= 85)
select s.s_id,s.s_name,round(avg_score, 2)
from student s
inner join t1 on s.s_id = t1.s_id;

-- 16.查询课程名称为"数学"，且分数低于60的学生姓名和分数
with
    t1 as (
    select s_id,s_score
    from score sc
    inner join course c on sc.c_id = c.c_id
    where c_name = '数学'
    and s_score < 60
    )
select s_name,s_score
from student s
inner join t1 on s.s_id = t1.s_id;

-- 17.查询所有学生的课程及分数情况
select
    s.s_id,
    s.s_name,
    sum(if(c_id = 1, s_score, 0)) as chinese,
    sum(if(c_id = 2, s_score, 0)) as math,
    sum(if(c_id = 3, s_score, 0)) as english
from student s
left join score sc
on s.s_id = sc.s_id
group by s.s_id,s.s_name;

-- 18.查询课程编号为01且课程成绩在80分以上的学生的学号和姓名
with
    t1 as (select s_id from score where c_id = 1 and s_score >= 80)
select s_id,s_name
from student s
where exists(select * from t1 where s.s_id = t1.s_id);

-- 19.求每门课程的学生人数
with
    t1 as (select c_id,count(*) as cnt_student from score group by c_id)
select c.c_id,c.c_name,cnt_student
from course c
inner join t1 on c.c_id = t1.c_id;

-- 20.查询选修"张三"老师所授课程的学生中，成绩最高的学生信息及其成绩
with
    t1 as (select c_id from course c inner join teacher t on c.t_id = t.t_id where t_name = '张三'),
    t2 as (select s_id,s_score from score sc inner join t1 on sc.c_id = t1.c_id order by s_score desc limit 1)
select s.*,s_score as max_score
from student s
inner join t2 on s.s_id = t2.s_id;





