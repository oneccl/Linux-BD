package com.SparkSQL.day0224

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/24
 * Time: 19:57
 * Description:
 */
object SparkSql {

  // SparkSQL:
  // 用于处理结构化数据，提供了新的数据抽象DataFrame和Dataset
  /*
  1、特点：兼容性(RDD/DataFrame/Dataset可互转)；统一的数据访问API；
  完全兼容Hive；标准的数据库连接(JDBC)
  2、使用：导入Spark-SQL依赖
  */

  // 程序入口：SparkSession
  def main(args: Array[String]): Unit = {
    // 1、创建SparkSession实例
    val spark:SparkSession = SparkSession.builder()
      .master("local[2]").appName("SparkSql")
      // 开启Hive支持（添加Spark on Hive依赖）:
      // SparkSession对象在调用sql方法时会查找集群中的Hive库表
      .enableHiveSupport().getOrCreate()

    // 2、SparkSQL输入
    // 使用SparkSession读取数据，创建DataFrame

    // 2.1、方式1：指定格式load
    val df:DataFrame = spark.read
      // 指定文件格式
      .format("text")
      .load("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    // 2.2、方式2：read读取text,csv,json,orc/parquet,jdbc(读取数据库：需要MySQL依赖driver)等
    val df1 = spark.read.text("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")

    // 3、创建临时视图（基于内存的临时表）
    df.createTempView("v1")
    // 4、显示数据（可指定limit）默认将1行作为1个字段，列名为：value
    //df.show(1)

    // 5、DF和DS有两类sql操作（数据清洗）
    // 5.1、DSL：逻辑操作
    // *需要导入隐式Hive函数包：
    import org.apache.spark.sql.functions.expr
    val df2:DataFrame = df.select(expr("split(value,' ')").as("arr"))
      // 后一个算子基于前一个的结果
      .select(
        // 列别名: expr.as()、expr.alias()、"col as alias"、"col alias"
        // 算子后：如select().as()、select().alias()，别名无效
        expr("arr[0]").as("ip"),
        expr("split(arr[3],':')[1]").alias("hour"),
        expr("arr[8] code"),
        expr("arr[9] as upload"),
        expr("arr[size(arr)-1] as download")
      )
      // 过滤
      .where("code==200")
    df2.show()

    println("******************")

    // 5.2、SQL：sql操作
    val sql =
      """
        |select
        |arr[0] ip,
        |split(arr[3],':')[1] hour,
        |arr[8] code,
        |arr[9] upload,
        |arr[size(arr)-1] download
        |from
        |(select split(value,' ') arr from v1)
        |where arr[8]==200
        |""".stripMargin
    // 此处不能写成where code==200，无法识别别名
    spark.sql(sql)
      .show()

    // 6、SparkSQL输出

    // 6.1、类型方法输出
    //df2.write.csv("C:\\Users\\cc\\Desktop\\SparkSql1")
    // 6.2、save：默认保存parquet类型
    //df2.write.save("C:\\Users\\cc\\Desktop\\SparkSql2")
    // 6.3、指定格式保存
    //df2.write.format("json").save("C:\\Users\\cc\\Desktop\\SparkSql3")

    // 6.4、保存到MySQL（需要MySQL依赖：Driver）
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    // 指定模式(覆盖Overwrite、追加Append等)保存到数据库（表自动生成）
    df2.write.mode(SaveMode.Overwrite)
      // 数据库连接四要素
      .jdbc("jdbc:mysql://localhost:3306/day0224_sparksql","logDetail",prop)

    // 常见文件格式
    // 存储空间比较(大~小)：json > csv = text >> parquet > orc
    // 查询效率比较(高~低)：parquet > orc >> json > csv > text

  }

}
