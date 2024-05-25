package com.SparkSQL.day0224

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/28
 * Time: 19:23
 * Description:
 */
object SparkSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkOnHive HQL").master("local[*]")
//      .config("hive.metastore.uris", "thrift://bd91:9083")
//      .config("spark.sql.warehouse.dir","hdfs://bd91:8020/user/hive/warehouse")
//      .config("hive.metastore.warehouse.dir","hdfs://bd91:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // SparkSource:

    // 1、HDFS
    val df:DataFrame = spark.read.text("hdfs://bd91:8020/Harry.txt")
    //df.createTempView("v1")
    // 例：SparkSQL单词统计
    // 1）regexp_replace(str,pattern,target): 正则匹配替换
    // 2）explode(array): 数组(列)转行
    var sql =
      """
        |select * from (
        |select
        |regexp_replace(t.w,'\\W+','') wd,sum(count) sum
        |from (
        |select
        |explode(split(value,'\\s+')) w,1 count
        |from v1) t
        |group by t.w
        |order by sum desc
        |limit 20 ) d
        |where length(d.wd)>0
        |""".stripMargin
    //spark.sql(sql).show()

    // 2、Hive表：Hive默认存储HDFS文件格式为TextFile

    // 1）方式1
    val ds:Dataset[String] = spark.read.textFile("hdfs://bd91:8020/user/hive/warehouse/usersinfo/000000_0")
    //ds.toDF().show()

    // 2）方式2: Spark on Hive（通过访问hive的metastore元数据的方式获取表结构信息和表数据）

    // Spark3.x默认的Hive版本是2.3.7（报错）
    // java.lang.UnsupportedOperationException: Unsupported Hive Metastore version (2.3.9)
//    val df1 = spark.sql("select * from hivecrud.logdetail")
//    df1.show()

    // 3）方式3: Spark通过jdbc连接Hive（添加依赖Hive-jdbc）可连接

    // (1)注册
    register()
    val prop = new Properties()
    prop.setProperty("driver","org.apache.hive.jdbc.HiveDriver")
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    // (2)连接
    val df2 = spark.read.jdbc(
      "jdbc:hive2://bd91:10000",
      "hivecrud.logdetail",
      prop
    )
    df2.show()
    // Row对象遍历：
    //df2.foreach(r => println(r.mkString(",")))

  }

  // 注册
  def register(): Unit ={
    JdbcDialects.registerDialect(new HiveDialect)
  }

}

// 解析字段值
class HiveDialect() extends JdbcDialect{

  // jdbc协议是否满足条件
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:hive2")
  }

  // 处理列名，解析值
  override def quoteIdentifier(colName: String): String = {
    // colName: 表名.fieldName
    val fieldName = colName.split("\\.")(1)
    s"$fieldName"
  }

}