package com.SparkCore.day0217

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/17
 * Time: 13:40
 * Description:
 */
object SparkCoreBroadcastVar {

  // （四）累加器、广播变量、RDD缓存、检查点

  def main(args: Array[String]): Unit = {
    // 2、广播变量（broadcast）: Worker(Executor)级别的多线程(Task/分区/核数)共享只读变量
    // 当Task数目较多时，Driver给每个Task发送一份数据(对象/变量)，此时Driver的带宽会成为
    // 系统的瓶颈，且会大量消耗Task服务器上的资源，若将该变量缓存为广播变量，则每个Executor
    // 仅拥有一份，该Executor启动的所有Task会共享该变量，大大节省了通信成本和服务器资源

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("BroadcastVar"))
    val rdd = sc.makeRDD(1 to 10)
    val list = rdd.collect().toList
    // 1）创建广播变量
    val broadcast = sc.broadcast(list)
    // 2）从公共缓存中读取对象
    val cacheList = broadcast.value
    val sum = cacheList.sum  // 求和
    println(sum)
    // 3）注意事项：
    // - 广播变量不能缓存RDD，RDD是不存储数据的
    // - 广播变量只能读，不能修改
    // - 若Executor端调用了Driver的非广播变量，则Executor有多少个Task就有多少份该变量副本
    // - 若Executor端调用了Driver的广播变量，则Executor缓存中有且只有一份该变量副本
    println("----------------")

    // 3、缓存（cache）：Worker(Executor)级别的热点RDD的缓存
    // RDD的缓存会随着任务的结束自动删除
    val acc = sc.longAccumulator
    val rdd1 = rdd.map(n => {
      acc.add(1)
      (n, 1)
    })
    // 1）开启缓存（缓存rdd1）
    // 底层调用了persist(StorageLevel.MEMORY_ONLY)，默认存储级别为：内存
    rdd1.cache()
    // 2）使用persist自定义存储级别（内存和磁盘）
    //rdd1.persist(StorageLevel.MEMORY_AND_DISK)
    // 开启cache和不开启cache分别执行以下2个行动算子
    rdd1.count()
    rdd1.collect()
    // 3）结论：
    // 不开启cache: 共执行20次，每个行动算子执行前map过程都得执行
    // 开启cache：共执行10次，当第2个行动算子执行时，直接拿取cache中的map结果rdd1
    println(acc.value)
    // 4）常用缓存级别（优点、缺点）
    /*
    - MEMORY_ONLY：内存足够的情况下，若内存不足会OOM(内存溢出)
    - MEMORY_AND_DISK：内存足够优先使用内存，内存不够时将数据缓存到磁盘
    - OFF_HEAP：将RDD对象存储在非JVM堆内存，原因是JVM中HEAP中的对象在回收(GC)时
      需要暂停任务线程，导致整体效率降低
    */
    println("----------------")

    // 4、检查点（checkpoint）：用于RDD的持久性操作
    // RDD的缓存会随着任务的结束自动删除，若计算过程中某些RDD需要永久保存到磁盘
    // 可以使用checkpoint；checkpoint在任务执行时相当于cache(DISK)级别的缓存
    // 任务结束后，checkpoint会保存为随机UUID，每次任务都单独生成文件
    val acc1 = sc.longAccumulator
    val rdd2 = rdd.map(n => {
      acc1.add(1)
      Thread.sleep(100)
      (n, System.currentTimeMillis())
    })
    // 1）设置检查点RDD的保存位置
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\checkpointRDD")
    // 2）检查点缓存rdd2
    rdd2.checkpoint()  // ② checkpoint第2次执行map,结果2
    // 3）执行过程: ①②③④
    println(rdd2.sortByKey(numPartitions = 1).collect().toList) // ① 第1次执行map,结果1
    println(acc1.value)  // 10
    acc1.reset()  // 累加器清0，否则以下Task共享累加器结果10

    println(rdd2.sortByKey(numPartitions = 1).collect().toList) // ③ 不再执行map,从缓存拿取结果2
    println(acc1.value)  // 0

    println(rdd2.sortByKey(numPartitions = 1).collect().toList) // ④ 不再执行map,从缓存拿取结果2
    println(acc1.value)  // 0

  }

}
