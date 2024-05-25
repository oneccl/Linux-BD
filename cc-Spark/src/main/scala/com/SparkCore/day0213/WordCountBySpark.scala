package com.SparkCore.day0213

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/13
 * Time: 18:11
 * Description:
 */
object WordCountBySpark {

  // Spark操作本地文件
  // 词频统计

  def main(args: Array[String]): Unit = {
    // 0、创建Spark配置文件对象
    val conf = new SparkConf()
    // 0.1、设置当前任务需要提交到的集群master地址（spark://bd91:7077）或
    // 设置当前任务需要提交到的本地环境，local作为master
    // - local[n] n为分配给当前任务的CPU核心数
    // - local[*] 将本地CPU所有核心数都分配给当前任务
    conf.setMaster("local[*]")
    // 0.2、设置应用名称
    conf.setAppName("WordCount")
    // 1、获取SparkCore程序入口对象
    val sc = new SparkContext(conf)
    // 2、读取文件，RDD（Resilient Distributed Dataset）弹性分布式数据集
    // EDD: 懒执行，遇到行动算子(action)程序才执行，否则程序不执行
    // Spark默认将文本使用\n分割，一行为一个元素
    val lines:RDD[String] = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt")
    // 3、单词统计
    // 3.1、lines:RDD[String] ==> words:RDD[String]
    // \\W+: 剔除[^a-zA-Z_0-9]
    // 转换算子（Transformation）
    lines.flatMap(_.toLowerCase.replaceAll("\\W+"," ").split("\\s+"))
      // 3.2、words->word:RDD[String] ==> 过滤
      .filter(!_.trim.equals("")).repartition(2) // 随时调整RDD分片数
      // 3.3、word ==> (word,1):RDD[(String,Int)]
      .map((_,1)).repartition(4) // 随时调整RDD分片数
      // 3.4、(word,[1,1,...]) ==> (word,n):RDD[(String,Int)]
      .reduceByKey(_+_)
      // 3.5、ascending: 升序or降序(默认true升序)
      // numPartitions: 分区数(若不设置，则按CPU核心数分区)
      .sortBy(_._2,ascending = false,1) // 参数调整RDD分片数
      // 3.6、输出
      // 行动算子（action）
      //.saveAsTextFile("C:\\Users\\cc\\Desktop\\temp\\WCBySpark")  // 输出到指定路径
      .foreach(println)  // 打印输出
  }

}
