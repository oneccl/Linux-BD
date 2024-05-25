package com.cc.day0315

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/16
 * Time: 15:09
 * Description:
 */
object TableCommonApi {

  // FlinkSQL 程序基本架构
  // Flink-MySQL

  def main(args: Array[String]): Unit = {
    // 1、创建表环境：主要负责：
    // 1）注册Catalog(目录：用于管理所有Database和Table的Metadata)和表；
    // 2）执行SQL查询
    // 3）注册用户自定义函数(UDF)
    // 4）DataStream与表之间的转换
    // 1.1、流式表环境创建
    // 方式1：流批一体：默认流处理
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableStreamEnv = StreamTableEnvironment.create(env)
    // 方式2：指定流处理模式
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableStreamEnv1 = TableEnvironment.create(settings)
    // 1.2、批处理环境创建
    // 方式1：流批一体：指定批处理
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val tableBatchEnv = StreamTableEnvironment.create(env)
    // 方式2：指定批处理模式
    val settings1 = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableBatchEnv1 = TableEnvironment.create(settings1)
    // 2、创建表：连接器表、虚拟表
    // A、连接器表（需要导入相关连接器依赖）
    // 2.1、Flink-JDBC
    // 2.1.1、Source
    // 字段按照名称和类型进行映射的，FlinkSQL中表字段和类型必须和数据库中
    // 保持一致(如varchar--String)，官网中有数据类型的映射关系
    // 使用with关键字指定连接到外部系统
    // 1）DDL 定义 输入表，读取数据
    var createDDL =
      """
        |-- 一次只能写一个SQL
        |create table source_mysql (
        |  name string,
        |  age int
        |) with (
        |  'connector' = 'jdbc',
        |  'url' = 'jdbc:mysql://localhost:3306/day0316_flinksql',
        |  'table-name' = 'stu',
        |  'username' = 'root',
        |  'password' = '123456'
        |)
        |""".stripMargin
    // executeSql 带连接器执行SQL
    val result:TableResult = tableStreamEnv.executeSql(createDDL)
    result.print()    // OK
    // 2）DQL 查询
    var selectDQL =
      """
        |select * from source_mysql
        |""".stripMargin
    // 4、执行SQL
    //val result1 = tableStreamEnv.executeSql(selectDQL)
    //result1.print()   // 表结构+数据
    // 5、执行查询，返回一个新表
    //val table = tableStreamEnv.sqlQuery(selectDQL)
    // B、虚拟表
    //tableBatchEnv.createTemporaryView("newTable",table)
    // 3、注册表：连接外部系统，输出数据
    // 2.1.2、Sink 注册表，写入数据
    var createDDL1 =
      """
        |create table sink_mysql(
        |  name string,
        |  num bigint
        |) with (
        |  'connector' = 'jdbc',
        |  'url' = 'jdbc:mysql://localhost:3306/day0316_flinksql',
        |  'table-name' = 'name_num',  -- 需要手动在数据库创建表
        |  'username' = 'root',
        |  'password' = '123456'
        |)
        |""".stripMargin
    //val result2 = tableStreamEnv.executeSql(createDDL1)
    //result2.print()    // OK
    // 3）DML 增删改
    var insertDML =
      """
        |insert into sink_mysql values
        |('a',178),
        |('b',188)
        |""".stripMargin
    //val result3 = tableStreamEnv.executeSql(insertDML)
    //result3.print()
    // | default_catalog.default_database.sink_mysql |
    // |                                          -1 |

  }

}
