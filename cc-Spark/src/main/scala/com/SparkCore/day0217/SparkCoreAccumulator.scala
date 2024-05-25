package com.SparkCore.day0217

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/17
 * Time: 13:38
 * Description:
 */
object SparkCoreAccumulator {

  // （四）累加器、广播变量、RDD缓存、检查点

  /*
  在Spark程序中，当一个传递给Spark操作的函数在远程节点上运行时，Spark操作实际上操作的是这个函数
  所用变量的一个独立副本；这些变量会被复制到每台机器上，且这些变量在远程机器上的所有更新不会传递会
  驱动程序；通常，跨任务的读写变量是低效的，因此，Spark提供了2种共享变量：累加器、广播变量
  */

  def main(args: Array[String]): Unit = {
    // 1、累加器（Accumulator）
    // 累加器在Driver端定义初始化，只能在Driver端读取最后的值，在Executor端更新

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Accumulator"))
    val rdd = sc.makeRDD(1 to 10)
    // 例：统计map算子执行了多少次

    // 方式1：值传递（无法计算）
    var count = 0  // 定义变量(变量是值传递)
    val rdd1 = rdd.map(n => {
      // 将该变量的值传递到每台机器的每个分区（Task）中执行
      count += 1
      (n, 1)
    })
    println(rdd1.collect().toList)
    println(count)  // 0: 值传递不会改变原数据
    // 结论：如果想通过创建在Driver中的变量统计RDD算子的执行次数，最终无法获取
    // 原因：RDD的算子操作是在Driver中编译，并真正提交到执行器(Executor)中的每个分区(Task线程)
    // 中执行，每个线程都会持有一份属于自己的变量，最终Driver(main方法)中的变量没有参与任何计算
    println("---------------")

    // 方式2：引用传递（创建对象，可以计算）
    // Spark默认提供了LongAccumulator、DoubleAccumulator等累加器对象
    // 用于方便的进行分布式聚合(计数)，也可以自定义对象继承AccumulatorV2
    // 1）创建，该方法底层调用了：
    val acc = sc.longAccumulator  // val acc = new LongAccumulator
                                  // register(acc)
    val rdd2 = rdd.map(n => {
      // add(对象)：将对象添加到累加器中
      acc.add(1)       // 统计累加次数
      //acc.add(n)     // 求和（和累加只能2选1）
      (n, 1)
    })
    println(rdd2.collect().toList)
    // 累加器输出对象
    println(acc.value) // 获取累加或求和结果
    println(acc.sum)   // 获取求和或累加结果
    // 结论：引用传递提交给执行器(Executor)中每个分区(Task线程)的是对象的地址
    // 每个线程操作的是同一份数据，最终修改了Driver(main方法)中的累加器对象的属性值
    println("---------------")

    // 方式3：自定义对象，继承AccumulatorV2
    val myAcc = new L2DAccumulator  // 创建
    sc.register(myAcc)              // 注册
    val rdd3 = rdd.map(n => {
      //myAcc.add(1)
      myAcc.add(n)
      (n,1)
    })
    println(rdd3.collect().toList)
    println(myAcc.value)   // 10.0
    println(myAcc.getSum)  // 55.0
    println(myAcc.avg)     // 5.5
  }

}
// 自定义对象，继承AccumulatorV2[输入类型,输出类型]，实现6个方法
// 参照LongAccumulator
class L2DAccumulator extends AccumulatorV2[Long,Double]{
  private var sum = 0L
  private var count = 0
  // 用于判断当前累加器是否为初始状态
  override def isZero: Boolean = sum==0 && count==0
  // 复制当前累加器的状态
  override def copy(): AccumulatorV2[Long, Double] = {
    val newAcc = new L2DAccumulator
    newAcc.sum=this.sum
    newAcc.count=this.count
    newAcc
  }
  // 重置当前累加器属性
  override def reset(): Unit = {
    this.sum=0
    this.count=0
  }
  // 将传入的对象添加到当前累加器属性（单个分区计算）
  override def add(v: Long): Unit = {
    this.count+=1
    this.sum+=v
  }
  // 将当前分区与其他分区的累加器值合并（合并多个分区结果合并）
  override def merge(other: AccumulatorV2[Long, Double]): Unit = {
    val acc = other.asInstanceOf[L2DAccumulator]
    this.count+=acc.count
    this.sum+=acc.sum
  }
  // 返回累加器的值
  override def value: Double = this.count.toDouble
  // 可用于求和
  def getSum: Double = this.sum.toDouble
  // 可用于求平均
  def avg : Double = this.sum.toDouble/this.count

}
