package com.day0303;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/7
 * Time: 14:01
 * Description:
 */
public class KafkaIdempotence {

    // Kafka数据一致性 幂等性(去重)
    // 幂等性：同一操作发起多次请求的结果是一致的(去重)

    // 3种语义
    /*
    1、At Most Once：最多一次(不重复会丢失) ACKS 0
    2、At Least Once：至少一次(不丢失会重复) ACKS 1
    3、Exactly Once：准确一次(不丢失不重复)= At Least Once + 幂等性
    */
    // Kafka数据丢失/重复情况
    /*
    1、生产者
    1）acks=0: 若网络抖动或消息体太大，Producer不去校验ACK，消息可能丢失
    2）acks=1(默认): 若Leader分区宕机，Follower分区还未同步数据，Leader未ACK，消息可能重复
    3）acks=-1/all: 可靠性最高（Leader和Follower都确认ACK）
    2、Broker
    采用多分区多副本机制，最大限度保证数据不丢失
    由于消息是先写入PageCache，定时顺序刷盘，若数据写入PageCache还未刷写到磁盘时
    Broker停电或宕机，且acks=0，消息可能丢失
    3、消费者（offset: 实时记录消费位置）
    1）自动提交offset(默认): 若先提交offset，后处理消息；若处理消息时异常/宕机
    由于offset已被记录，会导致消息丢失
    2）手动提交offset: 先处理消息，后提交offset；若在处理完消息提交之前发生异常/宕机
    由于offset未被记录，会导致消息重复
    */
    // Kafka数据丢失/重复解决
    /*
    1、生产者（丢失/重复）
    1）设置acks=-1，使用回调函数接收acks响应结果，对结果针对性处理（见Producer.java类）
    调整消息体大小，设置副本因子≥2，设置同步模式producer.type=sync
    2）使用Kafka-Producer提供的幂等性属性enable.idempotence=true(此时acks=-1)
    原理：Producer初始化时会被分配一个PID，发往同一Partition的消息会
    附带Sequence Number，Broker会对<PID, Partition, SeqNumber>缓存
    相同Sequence Number的消息只会持久化一条
    缺点：该幂等性仅解决单分区内的消息重复问题，无法保证跨分区的Exactly Once，且Producer重启PID会发生变化
    2、Broker（丢失）
    设置副本因子replication.factor≥3，若Leader异常，会从
    ISR(与Leader副本保持同步状态的副本集合)集合中重新选举Leader
    设置min.insync.replicas>1，消息至少被同步到多少个副本才算已提交
    确保replication.factor = min.insync.replicas + 1
    3、消费者（丢失/重复）
    设置enable.auto.commit=false手动提交位移，在消息被完整处理之后再手动提交位移
    实现幂等性(去重操作)，消息可以使用唯一标识去重
    选择唯一主键存储到Redis，先查询是否存在，若存在则不处理；若不存在，先插入Redis，再进行业务逻辑处理
    */

    public static void main(String[] args) {
        // 见cc-Flink模块 com.cc.day0316.OffsetManager单例类
    }

}
