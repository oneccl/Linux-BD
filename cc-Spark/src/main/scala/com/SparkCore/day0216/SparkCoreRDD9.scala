package com.SparkCore.day0216

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/16
 * Time: 18:28
 * Description:
 */
object SparkCoreRDD9 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 9、cartesian：笛卡尔积
    // repartition、coalesce：设置分区，指定是否Shuffle
    // pipe：执行一个脚本

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo5"))
    // 饮食搭配
    val rdd1 = sc.makeRDD(List("油泼面", "饺子", "快餐", "凉皮"))
    val rdd2 = sc.makeRDD(List("冰峰", "可乐", "雪碧"))
    // 9.1、cartesian: 获取两个RDD内容的所有匹配情况
    // RDD1[T] * RDD2[U] ==> RDD[(T,U)]
    // def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {}
    val rdd3 = rdd1.cartesian(rdd2)
    println(rdd3.collect().toList)  // 共3*4=12种结果

    // 例11：列举以下城市的所有可能的火车票情况
    val rdd4 = sc.makeRDD(List("西安", "北京", "上海", "武汉", "成都"))

    rdd4.cartesian(rdd4)
      // 9.2、repartition: 弹性增缩分区数
      // 底层调用了coalesce(numPartitions, shuffle = true)
      // 默认开启了Shuffle，数据会通过网络在机器间传输
      // - 优点：Shuffle后每台机器的数据比较均衡(不倾斜)
      // - 缺点：网络IO影响性能，降低了效率
      .repartition(2)
      // 9.3、coalesce: 可设置不开启Shuffle
      //.coalesce(2,false)
      .filter(t=>t._1 != t._2)  // 过滤相同K-V的
      .foreach(println)  // 共5*5=25 - 5 = 20

    // 9.4、pipe
    // pipe可以接收一个系统命令(shell命令、shell脚本、windows、dos命令)
    // 将原RDD中每一个元素传入命令或者脚本作为参数，并将脚本执行的输出结果收集到新的RDD返回

    // pipe是为了解决spark程序与Linux中所有其他程序的关联操作
    // 例如需要将c语言或shell编写的程序功能片段添加到Spark中
    // 则可以直接使用pipe将RDD的元素传入外部程序，并将外部程序的输出接收回RDD
    // 不需要考虑不同语言的兼容问题

    // 例12：使用pipe在Linux中执行如下脚本
    val rdd = sc.makeRDD(1 to 10)
    // 创建：/opt/pip.sh
    /*
    #!/bin/bash
    echo "开始执行"
    while read X
    do
    echo "接收到$X"
    done
    echo "任务结束"
    */
    rdd.pipe("/opt/pip.sh")  // 在Linux-shell中执行
      .collect()
  }

}
