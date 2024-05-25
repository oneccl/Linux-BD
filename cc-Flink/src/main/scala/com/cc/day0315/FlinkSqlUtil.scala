package com.cc.day0315

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.io.File
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/17
 * Time: 11:42
 * Description:
 */

object FlinkSqlUtil {

  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  private val tabEnv = StreamTableEnvironment.create(env)

  // 执行SQL
  def execSql(sql:String): TableResult ={
    // 创建表环境
    env.setParallelism(1)
    // 执行SQL
    val result:TableResult = tabEnv.executeSql(sql)
    result
  }

  // 通用读取外部.sql文件执行
  def exec(path:String): TableResult ={
    // 创建表环境
    env.setParallelism(1)
    // 读取外部.sql文件
    val list = FileUtils.readLines(new File(path), "UTF-8")
    // Java集合转Scala集合
    val ls:List[String] = list.asScala.toList
    val builder = new StringBuilder()
    ls.filter(line=>StringUtils.isNotBlank(line))
      .foreach(line=>{
        if (!line.contains(";")) builder.append(line).append("\n")
        else builder.append(line.replace(";", "\t")).append("\n")
      })
    val sql = builder.toString()
    println(sql)
    // 执行sql
    tabEnv.executeSql(sql)
  }

}
