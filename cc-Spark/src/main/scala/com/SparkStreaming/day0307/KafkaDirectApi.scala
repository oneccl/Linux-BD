package com.SparkStreaming.day0307

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/31
 * Time: 11:36
 * Description:
 */
object KafkaDirectApi {

  // Streaming数据一致性: Wal机制、Kafka-DirectAPI机制

  def main(args: Array[String]): Unit = {
    // Wal(Write Ahead Logs 预写入日志): 是Spark中的一个保障HA高可用的机制，HBase中也有应用
    /*
    1、问题描述：分布式系统中，假设某些节点宕机或网络异常，数据的完整性和一致性不能保障
    MR(可以将任务重新分配给其它节点)的数据是静态离线的，即使传输时数据丢失，也可以重发
    Spark容错：checkPoint机制，然而checkPoint保存的是任务的执行进度，跟数据的就收没有关系
    Spark中数据是分发到Executor上以Task为单位处理的，checkPoint介入的并不是这个数据的分发
    过程，而是Task的执行过程，当Driver进程宕机，所有的Executor进程也将宕机，这些进程所持有的
    内存中的数据将丢失
    2、Wal机制
    Wal是指一个存储系统，可以接收和存储数据，有时间属性和索引属性；Spark Wal用较小的log(记录数据操作)
    存储来代替数据存储，为避免存储log的机器宕机又出现数据丢失，Wal选用分布式文件存储系统HDFS
    3、Wal机制缺点
    1）log文件需要先存储到HDFS，后面的过程都需要等待log写入，减少了接收器的吞吐量，导致性能下降
    2）Wal虽然保证了数据一定不会丢失，但不能保证数据不会重复：Driver失败重启后，由于缓存中的数据
    可能没有写入到Wal日志中，该数据将被重发
    4、Spark容错总结
    原因：Kafka Receiver和ZK中的数据可能不同步，两个系统无法对已接收到的数据信息保存进行原子性操作
    1）checkPoint: 元数据检查点，保存Driver端批处理的元数据
    2）Wal: 数据检查点，将RDD中的数据保存到HDFS（解决数据已接收但未处理出现宕机丢失情况）
    4、Wal的开启（Spark默认未开启）
    */
    val spark = SparkSession.builder()
      .appName("CheckPoint Wal")
      .master("local[*]")
      // 开启Wal
      .config("spark.streaming.receiver.writeAheadLog.enable", "true")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(5))
    // 设置Wal日志存储到hdfs的路径
    ssc.checkpoint(s"hdfs://bd91:8020/spark/wal-log")

    // Kafka Direct API
    /*
    为解决Wal带来的性能损失，并保证exactly-once(一致性)语义，Streaming1.3引入了Kafka Direct API
    1、Kafka Direct机制:
    基于kafka原生的Consumer api直接操作Kafka底层的元数据信息，Kafka相当于一个文件系统
    所有offset对应的数据在Kafka中都已准备好，Spark Streaming直接追踪offset消费对应数据
    并保存到checkPoint，这样既保证了每条数据一定会被处理，且仅会被处理一次，又保证了数据一定
    是同步的，一定不会重复；这些偏移量信息也被可靠地存储（checkpoint），在从失败中恢复可以直接
    读取这些偏移量信息，完美的保证了事务的一致性
    2、优点
    1）不需要开启Wal，提高了效率，节省了磁盘空间
    2）若读取多个partition，Spark也会创建RDD多个partition，此时RDD的partition和Kafka
    的partition是一致的，极大的提高了性能
    3、缺点
    上述优点2）中repartition提高了并行度，但重新分区会产生shuffle
    4、Kafka Direct的使用
    见StreamingKafka.scala单例类
    */

  }

}
