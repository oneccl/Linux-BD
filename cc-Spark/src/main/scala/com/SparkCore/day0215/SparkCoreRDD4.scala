package com.SparkCore.day0215

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner, SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/15
 * Time: 14:33
 * Description:
 */
object SparkCoreRDD4 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 4、分区（mapPartitions、mapPartitionsWithIndex）、自定义分区

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo"))
    val rdd = sc.makeRDD(1 to 10)
    /*
    K-V的RDD[(K,V)]可以通过partitionBy()指定分区器
    Spark默认实现了2种分区器：
    - HashPartitioner: 根据Key的hashcode与分区数取模
      优点：将相同的Key分到同一分区，方便进行相关ByKey方法操作
      缺点：如果某些Key较多，会造成数据倾斜
    - RangePartitioner: 采用抽样算法，随机抽取并轮循分发数据到不同分区
      优点：每个分区元素相差无几，不会造成数据倾斜
      缺点：相同Key的数据分散在不同分区，若需要进行相关ByKey方法操作需要重新Shuffle
    */
    // 获取当前分区数(CPU核数、并发度、Task线程数)
    val ps = rdd.getNumPartitions
    println(ps)

    // 4.1、mapPartitions: 将数据按照当前分区数(CPU核数、并发度)进行拆分
    //   并将集合Iterator元素T类型转换为U类型
    // def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    rdd.mapPartitions(arr=>Iterator(arr.toList))
      .foreach(println)
    rdd.mapPartitions(arr=>Iterator(arr.mkString(",")))
      .foreach(println)

    // 4.2、mapPartitionsWithIndex: 将数据按照当前分区数进行拆分
    // 将集合Iterator元素T类型转换为U类型，并显示分区编号
    // def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    rdd.mapPartitionsWithIndex((index,arr)=>Iterator(index+" : "+arr.mkString(",")))
      .foreach(println)

    println("----------------")

    // 例4：统计用户访问量，按IP首数字分区
    val rdd1 = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    rdd1.map(_.split("\\s+").head)
      .map((_,1))
      .reduceByKey(_+_)
      // 1)使用HashPartitioner：Key.hashcode()%partitions
      //.partitionBy(new HashPartitioner(2))
      // 2)使用自定义分区器，按IP首数字分区
      .partitionBy(new MyPartitioner(9))
      // 显示分区编号
      .mapPartitionsWithIndex((index,arr)=>Iterator(index+" : "+arr.mkString(",")))
      .foreach(println)
  }

}

// 4.3、自定义分区器，参照HashPartitioner
class MyPartitioner(partitions:Int) extends Partitioner {

  override def numPartitions: Int ={
    partitions
  }

  override def getPartition(key: Any): Int = {
    // 取Key首数字%分区数
    // 字符'0'==>48
    (key.asInstanceOf[String].charAt(0)-'0')%partitions
  }

}