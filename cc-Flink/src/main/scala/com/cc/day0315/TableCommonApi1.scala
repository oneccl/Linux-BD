package com.cc.day0315

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/16
 * Time: 20:27
 * Description:
 */
object TableCommonApi1 {

  // Flink-Kafka

  def main(args: Array[String]): Unit = {
    // 创建表环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableStreamEnv = StreamTableEnvironment.create(env)
    // 2.2、Flink-Kafka
    // 2.2.1、Source 创建表 读取/消费Kafka数据
    // DDL
    var createDDL =
      """
        |create table kafka_source(
        |  line string
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'weblogs',
        |  'properties.bootstrap.servers' = 'bd91:9092,bd92:9092,bd93:9092',
        |  'properties.group.id' = 'group12',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json',                  -- 指定格式
        |  'json.ignore-parse-errors' = 'true' -- 解析错误使用null
        |)
        |""".stripMargin
    //tableStreamEnv.executeSql(createDDL)
    // DQL
    var selectDQL =
      """
        |select * from kafka_source
        |""".stripMargin
    //val result = tableStreamEnv.executeSql(selectDQL)
    //result.print()
    // 2.2.2、Sink 注册表 发送数据到Kafka
    // DDL
    var createDDL1 =
      """
        |create table kafka_sink (
        |  name string,
        |  age int
        |) WITH (
        |  'connector' = 'kafka',          -- 只支持追加的流
        |  'topic' = 'flinkSqlSink',       -- topic可自动生成
        |  'properties.bootstrap.servers' = 'bd91:9092,bd92:9092,bd93:9092',
        |  'format' = 'csv',               -- 指定格式
        |  'csv.field-delimiter'='\t'      -- csv格式数据的分割符
        |)
        |""".stripMargin
    tableStreamEnv.executeSql(createDDL1)
    // DML
    var insertDML =
      """
        |insert into kafka_sink values
        |('Tom',18),
        |('Jack',17)
        |""".stripMargin
    val result = tableStreamEnv.executeSql(insertDML)
    result.print()
  }

}
