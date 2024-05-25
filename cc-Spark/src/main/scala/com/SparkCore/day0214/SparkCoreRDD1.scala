package com.SparkCore.day0214

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/14
 * Time: 15:50
 * Description:
 */
object SparkCoreRDD1 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 1、map、sortByKey

    // 例1：统计日志文件各个小时的用户访问量
    val conf = new SparkConf().setMaster("local[2]").setAppName("LogAnalysis")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    rdd.map(_.split(" ")(3).split(":")(1))
      // 1.1、map: 转换
      // def map[U: ClassTag](f: T => U): RDD[U] = {}
      .map((_,1))
      // reduceByKey: 聚合，key相同的将值相加
      .repartition(4) // 设置4个分片(Task)
      .reduceByKey(_+_)
      // 1.2、sortByKey: 根据key排序(Spark没有默认排序，需要调用排序方法实现)
      // def sortByKey(
      //      ascending: Boolean = true,  默认升序
      //      numPartitions: Int = self.partitions.length  默认分区数=设置核心数
      // ): RDD[(K, V)] = {}
      .sortByKey(numPartitions = 1)  // 排序时分区为1
      // foreach: 遍历
      .foreach(println)
  }

}
