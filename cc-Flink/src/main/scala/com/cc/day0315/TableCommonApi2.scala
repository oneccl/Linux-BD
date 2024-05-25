package com.cc.day0315

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/17
 * Time: 10:12
 * Description:
 */
object TableCommonApi2 {

  // Flink-HDFS

  def main(args: Array[String]): Unit = {
    // 创建表环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableStreamEnv = StreamTableEnvironment.create(env)
    // 2.3、Flink-Hdfs
    // 2.3.1、Source 创建表 读取Hdfs文件
    // DDL
    var createDDL =
    """
      |create table source_hdfs (
      |  id int,
      |  name string,
      |  birth date,
      |  gender string
      |) with (
      |  'connector' = 'filesystem',       -- 指定连接器类型
      |  'path' = 'hdfs://bd91:8020/hive_exercises/student/student.csv',
      |  'format' = 'csv',                 -- 指定格式
      |  'csv.ignore-parse-errors'='true'  -- 解析错误使用null
      |)
      |""".stripMargin
    //tableStreamEnv.executeSql(createDDL)
    // DQL
    var selectDQL =
      """
        |select * from source_hdfs
        |""".stripMargin
    //val result = tableStreamEnv.executeSql(selectDQL)
    //result.print()
    /*
    | op |     id |     name |      birth |   gender |
    | +I | (NULL) |   s_name |     (NULL) |    s_sex |
    | +I |      1 |      赵雷 | 1990-01-01 |       男 |
    ...        ...       ...          ...        ...
    */
    // 2.3.2、Sink 注册表 写如Hdfs
    // 设置hadoop所属用户（否则报错）
    System.setProperty("HADOOP_USER_NAME","root")
    // DDL
    var createDDL1 =
      """
        |create table sink_hdfs (
        |  name string,
        |  price double,
        |  `hour` string
        |) partitioned by (`hour`) with (           -- 指定创建分区
        |  'connector' = 'filesystem',
        |  'path' = 'hdfs://bd91:8020/flinkSqlOut', -- 指定目录（自动创建目录）
        |  'format' = 'csv',
        |  'sink.rolling-policy.file-size' = '1M'   --滚动生成新的文件的大小，默认128M
        |)
        |""".stripMargin
    tableStreamEnv.executeSql(createDDL1)
    // DML
    var insertDML =
      """
        |insert into sink_hdfs values
        |('g1',58.0,'16'),
        |('g2',89.9,'17'),
        |('g3',66.6,'16')
        |""".stripMargin
    tableStreamEnv.executeSql(insertDML)
    // DQL 指定分区查询
    var selectDQL1 =
      """
        |select * from sink_hdfs where `hour`='17'
        |""".stripMargin
    val result = tableStreamEnv.executeSql(selectDQL1)
    result.print()
    /*
    | op |   name |  price |  hour |
    | +I |     g2 |   89.9 |    17 |
    */
    // 执行查询 返回一个新表
    val table:Table = tableStreamEnv.sqlQuery(selectDQL1)
    // 新表操作
    table.printSchema()

  }

}
