package com.cc.day0316

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/31
 * Time: 19:38
 * Description:
 */
object FlinkTwoPhaseCommit {

  // Flink-Kafka数据一致性: 2PC（Two Phase Commit 两阶段提交）
  // Flink中需要端到端Exactly-Once（精准一次处理）的位置有3个:
  /*
  1、Source: 需要外部源可重设数据的读取位置
  Kafka Consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障
  恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性
  当Flink故障时，Flink恢复状态时，会从上次Flink的Source保存的状态获取到上次消费的位置
  即上次检查点界限位置，并且从该位置消费kafka的数据
  Flink消费kafka数据时，在恢复状态时并不会使用Kafka自己维护的offset，若使用Kafka自己维护的offset
  当从Kafka消费的数据没有处理完时Flink出现故障，会丢失Flink中未处理的数据
  2、Flink内部端: Flink使用了一种轻量级快照机制检查点（checkpoint），保证一致性
  3、Sink: 需要外部系统能够提供一种手段允许提交或回滚Flink写入操作
  实现思想：事务写入，构建的事务对应的checkPoint，等checkPoint真正完成时，把所有对应结果写入Sink系统
  实现方式：
  1）Wal预写日志+幂等性（不推荐）：DataStream API提供了一个模板类：GenericWriteAheadSink
  存在问题：at-least-once（可能重复），可通过幂等性解决
  2）两阶段提交(2PC)
  (1)对于每个checkPoint，Sink任务会启动一个事务，并将所有接收的数据添加到事务里
  (2)将这些数据写入外部Sink，但不提交它们，这时只是"预提交"
  (3)当JobManager收到checkPoint完成的通知时，才正式提交事务，实现结果的真正写入
  此方式实现了exactly-once，它需要一个提供事务支持的外部Sink系统，Flink提供了TwoPhaseCommitSinkFunction接口
  */
  // 2PC步骤、过程
  /*
  1、第一条数据来后，开启一个Kafka事务，正常写入Kafka分区日志但标记为未提交
  读取Kafka-offset消费数据，Source保存消费到数据的offset，之后指定offset继续消费
  如果有任意一个Pre-Commit失败，所有的Pre-Commit都停止，Flink会回滚到最近成功的checkPoint
  2、JobManager触发checkPoint操作，barrier从Source开始向下传递，遇到barrier算子将状态存入状态后端，并通知JobManager
  3、Sink连接器收到barrier，保存当前状态，存入checkPoint，通知JobManager，并开启下一阶段的事务，用于提交下个检查点的数据
  4、JobManager收到所有任务的通知，发出确认信息，表示checkPoint完成，提交offset
  5、Sink任务收到JobManager的确认信息，正式提交这段时间的数据
  如果Commit失败(如网络故障)，Flink应用就会奔溃，然后根据用户重启策略进行重启重试
  6、外部Kafka关闭事务，提交的数据可以正常消费
  注意：1）Kafka超时时间要与Sink超时时间保持一致；2）Kafka设置未提交数据不可读
  */
  // 相关概念
  /*
  1、检查点(checkPoint)：有状态流应用的一致性检查点，其实就是所有任务的状态，在某个时间点的一份拷贝（一份快照）
  这个时间点，应该是所有任务都恰好处理完一个相同的输入数据的时候
  保存的状态其实是各个任务都处理完某个数据之后的一个状态
  Source的状态也需要保留，用来恢复Source中的数据偏移量，防止丢失数据
  2、分界线(barrier)：用来把一条流上数据按照不同的检查点分开
  分界线之前到来的数据导致的状态更改，都会被包含在当前分界线所属的检查点中
  而分界线之后的数据导致的所有更改，就会被包含在之后的检查点中
  3、状态后端(state backend)：用于保存checkPoint，默认是内存级的，也可以改为文件级持久化存储
  */

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1、开启周期性检查点(默认不开启)，检查点时间间隔1000毫秒，设置精确一次模式
    // Flink会定期保存检查点，若发生故障，会从最近一次检查点来恢复应用状态，重新启动
    env.enableCheckpointing(1000,CheckpointingMode.EXACTLY_ONCE)
    // 2、检查点配置
    val conf = env.getCheckpointConfig
    // 1）作业完成后是否保留检查点（持久化：作业结束依然保留）
    conf.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 2）检查点保存的超时时间
    conf.setCheckpointTimeout(60000)
    // 3）并发检查点数量，同时只能有1个检查点
    conf.setMaxConcurrentCheckpoints(1)
    // 4）启动不对齐检查点保存方式：可减少检查点保存的时间
    // 该设置要求检查点模式必须为exactly once；并发检查点数量必须为1
    conf.enableUnalignedCheckpoints()
    // 5）设置检查点保存到状态后端的位置
    conf.setCheckpointStorage("hdfs://bd91:8020/flink/checkpointDir")

    // 3、自动检查点的恢复
    // Flink提供了重启策略（restart strategy）使Job从最近一次checkpoint自动恢复现场
    // 3种重启策略（也可在flink-conf.yaml中配置）

    // 1）固定延时重启（fixed-delay）默认的
    // 当Flink Job失败时，该策略按照给定的固定间隔试图重启Job；若重启次数达到给定阈值后还未成功，就停止Job
    // 自定义重启状态
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        5,  // 正常运行之后，进入错误再运行的次数（阈值）
        Time.seconds(15)  // 延时（固定间隔）
      )
    )
    // 2）按失败率重启（failure-rate）
    // 当Flink Job失败时，该策略按照在给定的时间周期内按固定间隔重启Job；若重启次数达到给定阈值后还未成功，就停止Job
    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      10,  // 正常运行之后，进入错误再运行的次数（阈值）
      Time.minutes(5),  // 时间周期
      Time.seconds(15)  // 延时（固定间隔）
    ))
    // 3）直接失败
    env.setRestartStrategy(RestartStrategies.noRestart())

  }

}
