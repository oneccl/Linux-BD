package com.cc.day0316

import redis.clients.jedis.Jedis

import java.util

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/7
 * Time: 16:25
 * Description:
 */
object OffsetManager {

  // 1）Spark消费Kafka数据使用DirectAPI保证Exactly Once
  // 2）Flink消费Kafka数据通过自己维护Offset保证Exactly Once
  // Flink不推荐使用幂等性保证Exactly Once

  // 实现见cc-Spark com.SparkStreaming.day0308.OffsetManager单例类

}

