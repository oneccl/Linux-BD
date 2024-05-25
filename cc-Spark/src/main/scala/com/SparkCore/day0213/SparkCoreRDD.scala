package com.SparkCore.day0213

import org.apache.spark.SparkContext

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/13
 * Time: 19:15
 * Description:
 */
object SparkCoreRDD {

  // SparkCore是Spark计算框架的核心模块，负责完成分布式的批处理计算
  // SparkCore的应用程序，就是通过若干算子对RDD的操作

  // RDD（Resilient Distributed Dataset）弹性分布式数据集，是Spark的基本抽象
  // 特点：
  /*
  1、包含多个分区的集合(每个RDD可弹性设置分区)
  2、函数并行计算多个分区
  3、RDD前后存在依赖关系
  4、RDD支持为K-V（二元组）泛型的自定义分区器
  5、RDD只计算当前机器的每个切片，不会跨机器执行，尽量降低网络IO(Shuffle)
  6、RDD是一个不可变(只读)的分布式数据集
     读取上层RDD的数据，将计算完的结果写入到新的RDD中
  7、弹性：RDD的分区数量可以随时调整，RDD运行过程中，会为每个分区(分片)创建一个
     线程(Task)并行运算（线程数=分片数，一般≤机器内核数）
     - 随时调整并发度：RDD中的分区数对应Spark程序中的线程数(并发度)
     - 任务出错可以自动重试(容错性)：Spark任务计算出错时，首先会从上层RDD重新读取数据进行重试
       如果重试4次没有成功，则会将当前任务迁移到另一台机器(worker)尝试
  8、懒加载(懒执行)：RDD的算子分为
     - 转换算子(transformation)：将一个RDD转换成另一个RDD
     - 行动算子(action)：将RDD输出到文件、数据库或控制台
     当RDD程序只有转换算子时，不进行计算；只有出现行动算子时，RDD程序才真正开始执行
  */

  // RDD的4类操作

  // （一）创建RDD
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    // 1)通过文本文件创建
    val rdd1 = sc.textFile("")
    // 2)通过sequenceFile创建
    // [String,String] => [文件名，内容]
    val rdd2 = sc.sequenceFile[String, String]("")
    // 3)通过Scala集合创建(调用了parallelize())
    val rdd3 = sc.makeRDD(List(2, 3, 5, 7))
    val rdd4 = sc.parallelize(List(2, 3, 5, 7))
    // 4)通过Writable序列化的对象文件创建
    val rdd = sc.range(1, 10, 2)
    rdd.saveAsObjectFile("")
    val rdd5 = sc.objectFile("rdd序列化保存路径")

    println(rdd.collect().toList)
    rdd.saveAsTextFile("")
    rdd.saveAsObjectFile("")
  }

}
