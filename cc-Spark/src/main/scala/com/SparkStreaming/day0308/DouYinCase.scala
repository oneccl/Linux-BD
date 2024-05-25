package com.SparkStreaming.day0308

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/8
 * Time: 10:09
 * Description:
 */
object DouYinCase {

  // 1.数据来源 Java模拟
  // java包：com.SparkStreaming.day0308.DouYinDataSources

  // 2、数据采集 Flume
  // Sources taildir
  // Channels memory
  // Sinks Kafka

  // 3、数据存储 Kafka

  // 4、数据处理 SparkStreaming
  // 每2s从Kafka拉取一次数据

  // 5、数据输出&展示 MySQL&BI

  // 业务1：实时统计抖音用户热度全国榜(点赞总数)前10
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DouYin").master("local[2]").getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\StreamingDouYin")

    // 获取Streaming实时处理对象
    val ssc = new StreamingContext(sc,Seconds(2))
    // Kafka拉取数据配置
    val confMap = Map(
      ("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092"),
      ("group.id","g10"),
      ("auto.offset.reset","earliest")
    )
    // 获取kafka操作对象
    val ds:DStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // 轮循获取
      ConsumerStrategies.Subscribe[String, String]( // 订阅主题，获取数据
        Iterable("douyin_digg"),
        confMap
      )
    )
    // 数据计算，获取ds对象
    val ds1:DStream[(String,(String,Int))] = ds.map(_.value())
      // 数据清洗
      .map(line => {
        val cols = line.split("\t")
        val userId = cols(10)
        val nickName = cols(12)
        val city = cols(1)
        val workId = cols(0)
        val digg = cols(7).toInt
        val comment = cols(8).toInt
        val share = cols(9).toInt
        (userId,(nickName,digg))   // 复合元组类型处理
      })
      .reduceByKey((t1,t2)=>(t1._1,t1._2+t2._2))  // 聚合当前批数据
      // 累加历史数据，updateStateByKey[泛型]需要写泛型
      .updateStateByKey[(String,Int)]((values: Seq[(String,Int)], stateRes: Option[(String,Int)]) => {
        if (values.nonEmpty){
          val curVs:(String,Int) = values.reduce((t1, t2) => (t1._1, t1._2 + t2._2))  // 聚合当前批数据
          stateRes match {
            case None => Option(curVs)
            case _ => Option((curVs._1,curVs._2+stateRes.get._2))  // 累加历史数据
          }
        } else stateRes
      })

    // 数据保存到MySql
    val prop = new Properties()  // 数据库连接属性
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    ds1.foreachRDD((rdd:RDD[(String,(String,Int))])=>{
      // 升序
      val df = rdd
        .sortBy(_._2._2, ascending = false, numPartitions = 1)
        .map(t=>(t._1,t._2._1,t._2._2))   // DF匹配元组类型
        .toDF("userId", "nickName","digg")
      // 前10
      // 需要设置数据库的特殊字符编码格式utf8mb4，否则报错
      df.limit(10).write.mode(SaveMode.Overwrite).jdbc(
        "jdbc:mysql://localhost:3306/day0307_sparkstreaming?serverTimezone=UTC",
        "uw_digg",
        prop
      )
    })

    // 开启ds流
    ssc.start()
    ssc.awaitTermination()
  }

}
