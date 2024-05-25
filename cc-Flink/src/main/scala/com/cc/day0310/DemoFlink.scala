package com.cc.day0310

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/15
 * Time: 17:28
 * Description:
 */
object DemoFlink {

  // Flink
  /*
  1、Flink：是一个分布式计算框架，用于无边界和有边界数据流上的有状态计算
    Flink能在集群环境运行，并能以内存速度和任意规模进行计算
  2、无边界数据：流数据
    有边界数据：数据库、文件数据等
  3、批处理、流处理
  4、Flink核心概念
  1）JobManager: 作业管理器：主节点，client提交任务jar给JM，JM分发任务到所有TM
  2）TaskManager: 任务管理器：从节点，接收JM分发的任务，并占用Solt，启动任务进程
  3）Solt: 插槽：每个从节点执行任务的并发度
  */

  // Flink 与 Spark 区别
  /*
  1、设计理念：
  Spark: 微批（Micro-batch）处理，是一种伪实时
  Flink: 基于事件一行一行处理，是真正的流式计算
  2、架构：
  Spark: Master、Worker、Driver、Executor
  Flink: JobManager、TaskManager、Slot
  3、任务调度
  Spark: Streaming连续不断的生成微数据批次，构建有向无环图DAG，根据DAG中的Action操作
  形成Job，每个Job可以根据窄宽依赖生成多个stage
  Flink: 根据用户提交的代码生成StreamGraph，经过优化生成JobGraph，然后提交给JobManager
  进行处理，JobManager会根据JobGraph生成ExecutionGraph，ExecutionGraph是Flink调度最
  核心的数据结构，JobManager根据ExecutionGraph对Job进行调度
  4、时间机制
  Spark: Streaming支持的时间机制有限，只支持处理时间；使用processing time模拟event time
  必然会有误差，如果产生数据堆积的话，误差则更明显
  Flink: 支持三种时间机制：事件时间，注入时间，处理时间，同时支持watermark机制处理迟到的数据
  说明Flink在处理实时数据的时候更有优势
  5、容错机制（数据一致性）
  Spark: Streaming的容错机制是基于RDD的容错机制，会将经常用的RDD或对宽依赖加Checkpoint
  利用Streaming的Direct方式与Kafka可以保证数据输入、处理、输出过程的一致性（exactly once）
  Flink: 使用两阶段提交协议来保证exactly once
  6、吞吐量与延迟
  Spark: Spark是基于微批的，而且流水线优化做的很好，所以说它的吞入量是最大的，但是付出了延迟
  的代价，它的延迟是秒级
  Flink: Flink是基于事件的，消息逐条处理，而且它的容错机制很轻量级，所以它能在兼顾高吞吐量的同时
  又有很低的延迟，它的延迟能够达到毫秒级
  */

}
