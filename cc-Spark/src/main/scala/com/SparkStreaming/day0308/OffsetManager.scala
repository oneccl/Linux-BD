package com.SparkStreaming.day0308

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/11
 * Time: 15:55
 * Description:
 */
// 精确一次消费
/*
1）从Redis中读取offset
2）指定offset从Kafka消费数据
3）提取消费到的数据的offset
4）处理数据：结构转化(Json->Obj)、去重、维度关联
5）写入下游(Kafka、ES、HBase等)
6）保存/提交offset到Redis
*/
object OffsetManager {

  // 优化：精确一次消费
  // 问题：Kafka->消费数据->处理数据->（提交offset->写出数据(漏消费) / 写出数据->提交offset(重复消费)）
  // 管理方案：后置提交offset+下游幂等处理(使用ES幂等处理)：
  // 1）手动提交offset 2）手动提取Kafka-offset维护到Redis

  // offset管理维护-Redis

  // 步骤：
  // 1）从Kafka消费到数据，提取到offset（见Kafka2Spark2HBase单例类）
  // 2）数据成功写出到下游后，将offset存储到Redis
  // 3）下次从Kafka消费数据之前，先从Redis中读取offset，使用该offset到Kafka消费数据
  // 4）以此类推

  // Kafka offset的维护：groupId+topic+partition
  // Redis存储
  /*
  1）类型：hash
  2）key：group+topic
  3）value：partition-offset,...
  4）写入API：hset/hmset
  5）读取API：hgetAll
  6）是否过期：永不过期
  */

  // 存储offset到Redis
  def saveOffset(topic:String,groupId:String,offsets:Array[OffsetRange]): Unit ={
    if (offsets != null && offsets.length > 0){
      val redisValue = new util.HashMap[String, String]()
      for (elem <- offsets) {
        val partition = elem.partition    // 分区
        val endOffset = elem.untilOffset  // 当前消费到的offset
        redisValue.put(partition.toString,endOffset.toString)
      }
      val jedis = RedisUtil.getJedisFromPool
      val redisKey = s"offsets:$topic:$groupId"
      jedis.hset(redisKey,redisValue)

      // 设置key过期时间（秒）
      //jedis.expire("key",24*60*60L)
      // 查询key的过期时间
      //println(jedis.ttl("key"))
      // 清除key的过期时间，设置永不过期（不设置时间默认永不过期）
      //jedis.persist("key")

      jedis.close()
    }
  }

  // 读取offset到Redis
  // 如何让SparkStreaming通过指定offset进行消费？
  // ConsumerStrategies.Subscribe(topics,kafkaParams,offsets)可传入
  // 见Kafka2Spark2HBase单例类
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long] ={
    val jedis = RedisUtil.getJedisFromPool
    val redisKey = s"offsets:$topic:$groupId"  // redis-key
    import scala.collection.JavaConverters._
    // 根据key获取Redis中的Hash类型的值，并转为scala类型
    val redisValue = jedis.hgetAll(redisKey).asScala
    val offsets = mutable.Map[TopicPartition, Long]()
    // 转化
    for ((partition,offset) <- redisValue) {
      val tp:TopicPartition = new TopicPartition(topic, partition.toInt)
      offsets.put(tp,offset.toLong)
    }
    jedis.close()
    offsets.toMap
  }

}
