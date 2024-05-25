package com.SparkSQL.day0224

import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/26
 * Time: 17:01
 * Description:
 */
object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local[2]").appName("Demo").getOrCreate()
    val df = spark.read.format("text").load("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    df.createTempView("v1")  // 创建临时视图

    // 例1：统计每个小时的用户访问量排行(code=200)
    val sql =
      """
        |select
        |hour,count(1) count
        |from
        |(select
        |split(split(value,'\\s+')[3],':')[1] hour
        |from v1
        |where split(value,'\\s+')[8]==200)
        |group by hour
        |order by count desc
        |""".stripMargin
    //spark.sql(sql).show()

    // 例2：统计每个用户一天的各数据总量(code=200)
    spark.udf.register("date_format",DateUtil.dateFormat)  // 注册函数
    val sql1 =
      """
        |select
        |   ip,
        |   count(ip) count,
        |   sum(upload) upSum,
        |   sum(download) downSum
        |from (
        |-- 数据清洗
        |select
        |   arr[0] ip,
        |   date_format(substr(arr[3],2,11)) day,
        |   arr[8] code,
        |   cast(arr[9] as bigint) upload,
        |   cast(arr[size(arr)-1] as bigint) download
        |from
        |(select split(value,'\\s+') arr from v1)
        |where arr[8]==200
        |) ld
        |group by ip
        |""".stripMargin
    //spark.sql(sql1).show()

    // 例3：统计当天的各数据总量
    val sql2 =
      """
        |select
        |   day,
        |   count(1) dayUserCount,
        |   sum(code200) code200Count,
        |   sum(upload) dayUpSum,
        |   sum(download) dayDownSum
        |from (
        |select
        |   date_format(substr(arr[3],2,11)) day,
        |   if(arr[8]==200,1,0) code200,
        |   cast(arr[9] as bigint) upload,
        |   cast(arr[size(arr)-1] as bigint) download
        |from
        |(select split(value,'\\s+') arr from v1)
        |)
        |group by day
        |""".stripMargin
    spark.sql(sql2).show()

  }

}
