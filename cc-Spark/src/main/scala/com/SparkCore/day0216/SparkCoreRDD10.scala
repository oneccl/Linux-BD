package com.SparkCore.day0216

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/16
 * Time: 19:13
 * Description:
 */
// Spark相关概念
/*
Job：以action算子为界，一个action触发一个Job
Stage：Job的子集，以RDD宽依赖为界，遇到宽依赖即划分Stage
Task：Stage的子集，以分区数来衡量，分区数=Task任务数
窄依赖：一个父RDD的分区只能被子RDD的一个分区使用：父分区:子分区=>1:1
宽依赖：一个/多个子RDD的分区依赖一个父RDD的的多个/1个分区：父分区:子分区=>多:1/1:多/多:多（会产生Shuffle）
DAG(有向无环图)：RDD通过一系列转换形成DAG，根据RDD间的依赖关系将DAG划分成不同的Stage
*/
object SparkCoreRDD10 {

  // （三）RDD行动算子（action）
  // RDD.转换算子 => !RDD

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo6"))
    val rdd = sc.makeRDD(List(5,7,2,3,4))

    // 1）reduce：聚合
    // def reduce(f: (T, T) => T): T = {}
    val sum:Int = rdd.reduce(_ + _)
    println(sum)

    // 2）collect：收集到数组
    val arr:Array[Int] = rdd.collect()
    println(arr.toList)

    // 3）count：计数
    val count:Long = rdd.count()
    println(count)

    // 4）first：取第一个元素
    val f:Int = rdd
      .sortBy(x=>x,false,1)
      .first()  // 取最大值元素
    println(f)

    // 5）max/min：最大值，最小值
    val max:Int = rdd.max()
    val min:Int = rdd.min()
    println(max)
    println(min)

    // 6）take(n)：取前n个元素
    val arr1:Array[Int] = rdd.take(2)
    println(arr1.toList)

    // 7）takeOrdered(n)：先对元素排序，再取前n个
    val arr2:Array[Int] = rdd.takeOrdered(3)
    println(arr2.toList)

    // 8）takeSample：从RDD中随机抽取num个元素
    // 参数1：是否重复抽取 参数2：取样数 参数3：种子(几种结果)
    val arr3:Array[Int] = rdd.takeSample(false, 3, 1)
    println(arr3.toList)

    // 9）countByKey：按照key计数
    val rdd1 = sc.makeRDD(List("k1" -> 2, "k2" -> 1, "k2" -> 3))
    val map:scala.collection.Map[String,Long] = rdd1.countByKey()
    println(map)  // Map(k1 -> 1, k2 -> 2)

    // 10）foreach：遍历元素
    rdd1.foreach(println)

    // 11）将RDD输出到文件
    rdd.saveAsTextFile("")
    rdd.saveAsObjectFile("")
    // 只有二元组作为泛型的RDD才能保存成hadoop序列文件:
    rdd.map(x => (x, x)).saveAsSequenceFile("")

  }

}
