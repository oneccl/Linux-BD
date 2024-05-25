package com.SparkSQL.day0224

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Locale


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/26
 * Time: 16:23
 * Description:
 */
object SelfDefineFunc {

  // SparkSQL自定义函数及使用

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("Func").getOrCreate()
    // 2、注册函数
    spark.udf.register("date_format",DateUtil.dateFormat)
    // 3、使用函数
    val df = spark.read.text("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    // 将含有日期字段的截取并格式化
    df.createTempView("v1")
    val sql =
      """
        |select
        |date_format(substr(split(value,' ')[3],2,11)) date
        |from v1
        |""".stripMargin
    spark.sql(sql).show(1)
  }

}

// 1、自定义函数类
object DateUtil{

  // 31/Oct/2021（US）=> 2021-08-31

  // 函数定义：def 变量名 = (形参列表)=>{函数体}
  def dateFormat = (dt:String) => {
    // 解析日期: 指定国家
    val sdf = new SimpleDateFormat("dd/MMM/yyyy",Locale.US)
    val date = sdf.parse(dt)  // 解析: Sun Oct 31 00:00:00 CST 2021
    // 格式化日期
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
    sdf1.format(date)
  }

}
