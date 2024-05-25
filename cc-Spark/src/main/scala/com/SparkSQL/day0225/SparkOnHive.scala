package com.SparkSQL.day0225

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/12/6
 * Time: 22:21
 * Description:
 */
object SparkOnHive {

  def main(args: Array[String]): Unit = {
    // Spark on Hive（通过访问hive的metastore元数据的方式获取表结构信息和表数据）

    // Spark on Hive：Hive只作为存储，Spark负责sql解析优化计算
    // Hive on Spark：Hive既作为存储又负责sql的解析优化，Spark负责计算

    // 1）在本地运行，将hive-site.xml文件拷贝到resource目录下
    // 2）SPARK_HOME/conf/下添加：hive-site.xml
    /*
    <!-- 用于远程连接hive metastore服务 -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://bd91:9083</value>
    </property>
    */

    // 测试：

    val spark = SparkSession.builder()
      .appName("SparkOnHive HQL").master("local[*]")
      .config("hive.metastore.uris", "thrift://bd91:9083")
      .config("spark.sql.warehouse.dir", "C:\\Users\\cc\\Desktop\\spark\\warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val df1 = spark.sql("select * from hivecrud.logdetail")
    df1.show()

    // 结果：
    // Unsupported Hive Metastore version (2.3.9). Please set spark.sql.hive.metastore.version with a valid version.


  }

}
