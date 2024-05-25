package com.SparkStreaming.day0306

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/6
 * Time: 16:56
 * Description:
 */
object 流式处理方案 {

  def main(args: Array[String]): Unit = {
    // 统计每小时用户访问量变化趋势

    // 1、数据来源
    // Java模拟 java包：com.SparkStreaming.day0307.DataSources

    // 2、数据采集 Flume
    // Source: tailDir
    // Channel: 内存
    // Sink: Kafka

    // 3、数据存储 Kafka
    // topic: weblogs

    // 4、数据处理（实时计算） SparkStreaming
    // 每2s从Kafka拉取一次数据
    // 见day0307.StreamingKafka单例类

    // 5、数据输出 MySQL

    // 6、数据展示 BI、EChart

  }

}
