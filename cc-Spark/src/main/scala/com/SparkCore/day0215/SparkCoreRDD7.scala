package com.SparkCore.day0215

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/15
 * Time: 17:43
 * Description:
 */
object SparkCoreRDD7 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 7、combineByKey 预聚合；reduceByKey：根据K聚合V

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo3"))
    val rdd = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")

    // 例7：统计用户访问量排行，使用预聚合
    rdd.map(_.split("\\s+").head)
      .map((_,1))
      // (1)使用reduceByKey
      // 本质上就是combineByKey在聚合过程中不改变RDD[(String,Int)]中V的类型
      // def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {}
      .reduceByKey((v1,v2)=>v1+v2)
      // (2)使用combineByKey
      // Spark中无论使用reduceByKey聚合还是使用groupByKey分组，底层都调用了combineByKey
      // def combineByKey[C](
      //      createCombiner: V => C,     给第一个V创建一个聚合器（聚合器对象可以是任意类型）
      //      mergeValue: (C, V) => C,    将其他V与上一次计算完的C进行合并（每个分区预聚合）
      //      mergeCombiners: (C, C) => C 将多个聚合器的结果进行合并（最终聚合）
      //      ): RDD[(K, C)] = {}
      .combineByKey(
        // 拿到第一个V，创建聚合器对象（函数参数类型需要声明，下同）
        (v:Int)=>v,
        // 拿到v=>v的结果作为c，每个分区预聚合
        (c:Int,v:Int)=>c+v,
        // 拿到多个(c,v)=>c+v的结果作为c1、c2，合并多个聚合器结果
        (c1:Int,c2:Int)=>c1+c2,
        // 可弹性指定分区数（不指定默认按照给定的CPU核心数进行分区）
        2
      )
      .mapPartitions(arr=>Iterator(arr.toList))  // 分区显示
      //.foreach(println)

    // 例8：统计每个用户的访问次数、访问成功次数(code:200)、请求流量总和
    // 方式1：元组==>reduceByKey、combineByKey
    rdd.map(line => {
      val cols = line.split("\\s+")
      val ip = cols.head
      val code200 = if (cols(8).equals("200")) 1 else 0
      val upload = try cols(9).toLong catch {case e: Exception => 0L}
      (ip, (1, code200, upload))  // 元组
    })
      // (1)直接使用reduceByKey
      //.reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2,v1._3+v2._3))
      // (2)使用combineByKey
      .combineByKey(
      (v:(Int,Int,Long))=>v,
      (c:(Int,Int,Long),v:(Int,Int,Long))=> (c._1+v._1,c._2+v._2 ,c._3+v._3),
      (c1:(Int,Int,Long),c2:(Int,Int,Long))=> (c1._1+c2._1,c1._2+c2._2,c1._3+c2._3)
      )
      .sortBy(_._2._3,false,1) // 指定字段排序
      //.foreach(println)

    // 方式2：封装对象==>reduceByKey、combineByKey
    val rdd1 = rdd.map(line => {
      val cols = line.split("\\s+")
      val ip = cols.head
      val code200 = if (cols(8).equals("200")) 1 else 0
      val upload = try cols(9).toLong catch {case e: Exception => 0L}
      (ip, LogDetails(1, code200, upload)) // 对象
    })
      // (1)直接使用reduceByKey
      //.reduceByKey((l1,l2)=>l1.sum(l2))
      // (2)使用combineByKey
      .combineByKey(
        (l: LogDetails) => l,
        (l1: LogDetails, l2: LogDetails) => l1.sum(l2),
        (c1: LogDetails, c2: LogDetails) => c1.sum(c2)
      )
      .sortBy(_._2.upload, false, 1) // 指定字段排序
    //rdd1.foreach(println)

    // 例9：基于例8结果，统计单次正常访问(code=200)的平均流量
    // (ip,(ipCount,code200,upload,upAvg=upload/code200))
    rdd1.map(t=>{
      // 处理code200为0的异常
      val upAvg:Double = try t._2.upload / t._2.code200 catch {case e:Exception=>0}
      (t._1,(t._2.ipCount,t._2.code200,t._2.upload,upAvg))
    }).sortBy(_._2._4,false,1) // 根据平均流量排序
      .foreach(println)
  }

}

// 案例类，封装对象
case class LogDetails(var ipCount:Int,var code200:Int,var upload:Long){

  // 属性求和
  def sum(ld:LogDetails): LogDetails ={
    ipCount+=ld.ipCount
    code200+=ld.code200
    upload+=ld.upload
    this
  }

}