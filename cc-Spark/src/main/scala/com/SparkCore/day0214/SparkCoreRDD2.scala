package com.SparkCore.day0214

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/14
 * Time: 16:11
 * Description:
 */
object SparkCoreRDD2 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 2、filter、sortBy

    // 例2：统计西安大数据开发相关岗位学历要求占比
    val conf = new SparkConf().setMaster("local[2]").setAppName("MakeUp")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D:\\JavaProjects\\Linux-BD\\cc-Spark\\src\\main\\resources\\srcData\\bigdata.txt")
    // 方式1：
    // 1）先统计各学历数量
    val list = List("本科", "大专", "硕士", "学历不限")
    // 2.1、filter: 过滤
    // def filter(f: T => Boolean): RDD[T] = {}
    var top = rdd.filter(_.split("\t").length > 5) // length范围:1 3 10 12; 正常12列取第6列
      // map: 转换
      .map(line => {
        val cols = line.split("\t")
        // 正常12列取第6列，10列的取第3列
        if (list.contains(cols(5))) (cols(5), 1) else (cols(3), 1)
      })
      // 剔除第一行(标题行)
      .filter(t => list.contains(t._1))
      // 根据K聚合，key相同的将值相加，设置分片4
      .reduceByKey(_ + _).repartition(4)
      // 2.2、sortBy：对指定字段排序
      // def sortBy[K](
      //      f: (T) => K,  排序字段
      //      ascending: Boolean = true,  升序or降序
      //      numPartitions: Int = this.partitions.length  分区数
      //      ): RDD[T] = {}
      // 排序，降序，设置分区1
      .sortBy(_._2, false, 1)
      //.foreach(println)

    // 获得所有学历总次数
    val total:Double = top.collect().map(_._2).sum

    // 2）统计各学历占比
    // Java将Double格式化(保留2位)输出：String.format("%.2f", 3.1415926)
    // 此处不能使用Java的Double格式化（报错）
    // Scala将Double格式化(保留2位)输出：Double.box(3.1415926).formatted("%.2f")
    // 需要指定total为Double类型，否则结果为0*任何数=0
    top.map(t=>(t._1,t._2,Double.box(t._2/total*100).formatted("%.2f")+"%"))
      //.foreach(println)

    // 方式2：

    // 1）先获得所有出现的学历
    val topEB = rdd.filter(_.split("\t").length > 5)
      .map(line => {
        val cols = line.split("\t")
        if (list.contains(cols(5))) cols(5) else cols(3)
      })
      .filter(t => list.contains(t))  // 剔除第一行(标题行)

    // 根据所有学历(key)计数，获得各学历总次数
    val totalEB:Double = topEB.count()

    // 2）统计各学历总数及占比
    topEB.map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false,1)
      .map(t=>(t._1,t._2,Double.box(t._2/totalEB*100).formatted("%.2f")+"%"))
      .foreach(println)
  }

}
