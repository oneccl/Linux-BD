package com.SparkCore.day0215

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/15
 * Time: 16:43
 * Description:
 */
object SparkCoreRDD5 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 5、抽样计算（sample）

    // 由于数据处理通常面临海量数据，每次使用全量数据进行测试效率较低
    // 但如果选择部分数据进行测试，有可能选择的样本不具有代表性，导致结果偏差较大
    // 需要根据要求的比例，从全量数据中抽取部分数据，作为样例
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo1"))
    val rdd = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\weblogs")
    println("全量数据(行): "+rdd.count())

    // def sample(
    //      withReplacement: Boolean,  是否重复抽样
    //      fraction: Double,          抽样比例
    //      seed: Long = Utils.random.nextLong): RDD[T]  种子(几种样例)

    // seed种子：样例数为1种，测试输出多次，结果不变
    val rdd1 = rdd.sample(false, 0.005, 1)
    println("抽样数据(行): "+rdd1.count())

    // 例5：使用抽样数据：统计用户访问量排行
    rdd1.map(_.split("\\s+").head)
      .map((_,1))
      .reduceByKey(_+_)
      .sortBy(_._2,false,1)
      .foreach(println)

    // 测试seed种子：Random(1)：样例为1种，结果始终不变
    seedTest(1)

  }

  def seedTest(seed:Int): Unit ={
    val random = new Random(seed)
    for (_ <- 1 to 5){
      println(random.nextInt(10))
    }
  }

}
