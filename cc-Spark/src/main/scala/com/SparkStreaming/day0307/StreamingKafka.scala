package com.SparkStreaming.day0307

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/7
 * Time: 17:00
 * Description:
 */
object StreamingKafka {

  // Streaming消费/拉取Kafka数据
  // 业务：统计各小时访问量变化

  def main(args: Array[String]): Unit = {
    // 创建SparkSQL操作对象
    val spark = SparkSession.builder()
      .appName("demo").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    // 设置检查点
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\StreamingCP")
    // 导入转换包
    import spark.implicits._

    // 获取实时处理对象，批处理时间间隔2s
    val ssc = new StreamingContext(sc,Seconds(2))

    // 消费者配置
    val confMap = Map(
      ("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092"),
      ("group.id","g11"),
      ("auto.offset.reset","earliest")
    )
    // def createDirectStream[K, V](
    //      ssc: StreamingContext,     实时处理对象
    //      locationStrategy: LocationStrategy,       读取分区数据策略
    //      consumerStrategy: ConsumerStrategy[K, V]  消费者配置
    //    ): InputDStream[ConsumerRecord[K, V]] = {}
    val ds:DStream[ConsumerRecord[String,String]] =
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,    // 轮循获取
      // def Subscribe[K, V](
      //      topics: Iterable[String],
      //      kafkaParams: collection.Map[String, Object]):
      // 订阅主题，获取数据
      ConsumerStrategies.Subscribe[String,String](
        Iterable("weblogs"),  // 主题
        confMap   // 配置
      )
    )

    //ds.print()  // 打印的是ConsumerRecord对象，报错

    // Streaming拉取Kafka数据：
    // 打印ConsumerRecord中的数据
    //ds.map(_.value()).print()

    // 获取各小时访问量
    val ds1:DStream[(String,Int)] = ds.map(_.value())
      .map(line => {
        val cols = line.split("\\s+")
        val hour = cols(3).split(":")(1)
        (hour, 1)
      })
      .reduceByKey(_ + _) // 当前批处理数据聚合
      .updateStateByKey((values: Seq[Int], stateRes: Option[Int]) => { // 累加历史数据
        stateRes match {
          case None => Option(values.sum)
          case _ => Option(values.sum + stateRes.get)
        }
      })
    // def foreachRDD(foreachFunc: RDD[T] => Unit): Unit = {}

    val prop = new Properties()  // 数据库连接属性
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    // 保存数据到数据库（模式：覆盖）
    ds1.foreachRDD(rdd=>{
      val df = rdd.sortByKey(numPartitions = 1).toDF("hour", "uc")
      df.write.mode(SaveMode.Overwrite).jdbc(
        "jdbc:mysql://localhost:3306/day0307_sparkstreaming?serverTimezone=UTC",
        "hour_uc",
        prop
      )
    })

    // 启动ds
    ssc.start()
    ssc.awaitTermination()

  }

}
