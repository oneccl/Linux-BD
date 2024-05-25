package com.SparkSQL.day0225

import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/30
 * Time: 20:02
 * Description:
 */
object SparkSqlUtil {

  // 获取SparkSQL执行对象
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Spark")
    .enableHiveSupport()
    .getOrCreate()

  // 执行SQL
  def execSql(sql:String): DataFrame ={
    val df:DataFrame = spark.sql(sql)
    df
  }

  // 通用读取外部.sql文件执行
  def exec(path:String): DataFrame ={
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
    val df:DataFrame = spark.sql(sql)
    df
  }

}
