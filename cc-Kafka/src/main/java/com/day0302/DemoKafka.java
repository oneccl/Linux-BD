package com.day0302;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/3
 * Time: 15:06
 * Description:
 */
public class DemoKafka {

    // Kafka: 消息队列、事件流平台

    // 1、作用：用于构建数据管道、流式分析应用和关键任务的管理(消息驱动)

    // 2、组件及原理：见kafka.png

    // 3、数据存储机制（/opt/module/kafka-2.4.1/kafka-logs/）

    // 发送数据次数                           1(Java) 2(MR)   ...

    // 1）[次数单调递增序列(从0开始)、开始位置、长度]：offset+partition
    // 00000000000000000000.index           [0,0,4] [1,5,2] ...
    // 2）存储真实数据
    // 00000000000000000000.log             JavaMR...
    // 3）存储每次时间戳
    // 00000000000000000000.timeindex       [0,t1] [1,t2]   ...

}
