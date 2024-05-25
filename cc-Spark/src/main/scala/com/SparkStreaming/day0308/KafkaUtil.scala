package com.SparkStreaming.day0308

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/11
 * Time: 17:18
 * Description:
 */
object KafkaUtil {

  // 消费者配置
  private var confMap: mutable.Map[String, String] = mutable.Map(
    ("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
    ("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
    ("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092"),
    ("auto.offset.reset","earliest"),
    ("enable.auto.commit","false")
  )

  private var producer: KafkaProducer[String,String] = _

  // 生产者

  // 获取生产者对象
  def getKafkaProducer:KafkaProducer[String,String]={
    // 生产者配置
    val proConf:util.HashMap[String,AnyRef] = new util.HashMap()
    // Kafka集群
    proConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bd91:9092,bd92:9092,bd93:9092")
    // K-V序列化
    proConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    proConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    // acks
    proConf.put(ProducerConfig.ACKS_CONFIG,"all")
    // 幂等性
    proConf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")
    producer = new KafkaProducer[String, String](proConf)
    producer
  }

  // 发送消息
  def send(topic:String,data:String): Unit ={
    producer.send(new ProducerRecord[String, String](topic, data))
  }

  // 刷新Kafka缓存
  def flush(): Unit ={
    producer.flush()
  }

  // 消费者

  // 基于SparkStreaming消费，获取KafkaDStream，使用默认的offset
  def getKafkaDStream(ssc:StreamingContext,topics:Array[String],groupId:String): DStream[ConsumerRecord[String,String]] ={
    confMap.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    val ds = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](
        topics,
        confMap
      )
    )
    ds
  }

  // 基于SparkStreaming消费，获取KafkaDStream，使用指定的offset
  def getKafkaDStream(
      ssc:StreamingContext,
      topics:Array[String],
      groupId:String,
      offsets:Map[TopicPartition,Long]): DStream[ConsumerRecord[String,String]] ={
    confMap.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    val ds = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](
        topics,
        confMap,
        offsets
      )
    )
    ds
  }

}
