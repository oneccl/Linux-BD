package com.SparkCore.day0215

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/15
 * Time: 17:20
 * Description:
 */
object SparkCoreRDD6 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 6、并集（union）、交集（intersection）、补集（subtract）、distinct（去重）
    // groupByKey

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo2"))
    val rdd1 = sc.makeRDD(1 to 10)
    val rdd2 = sc.makeRDD(6 to 15)

    // 6.1、union: rdd1与rdd2并集
    val rdd3 = rdd1.union(rdd2)
    println(rdd3.collect().toList)

    // 6.2、intersection: rdd1与rdd2交集
    val rdd4 = rdd1.intersection(rdd2)
    println(rdd4.collect().toList)

    // 6.3、subtract: rdd1的补集，属于rdd1但不属于rdd2（rdd1-rdd2）
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().toList)

    // 6.4、distinct：去重
    val rdd6 = rdd3.distinct()
    println(rdd6.collect().toList)

    println("----------------")

    // 6.5、groupByKey: 根据Key分组
    // 例6：统计用户访问量排行，分组显示
    val rdd7 = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    rdd7.map(_.split("\\s+").head)
      .map((_,1))
      // def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
      // 底层使用HashPartitioner按照分区数/核心数/Task数进行分区
      // 并调用combineByKey()先预聚合再最终聚合（见SparkCoreRDD7.scala文件）
      // RDD[(String,Int)] ==>  RDD[(K, Iterable[V])]
      // (ip,1)(ip,1)...   ==>  (ip,[1,1,...])

      // 以下2句相当于：partitionBy(new HashPartitioner(2))
      .groupByKey(2)  // 此处可指定分区数
      .map(t=>(t._1,t._2.sum))  // 对每个分组/分区都执行该函数

      // 分区显示
      .mapPartitions(arr=>Iterator(arr.toList))
      .foreach(println)

  }

}
