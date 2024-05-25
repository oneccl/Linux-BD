package com.SparkStreaming.day0307

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/13
 * Time: 13:35
 * Description:
 */
object DStreamOperation {

  // Streaming 输入、算子、输出

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("Operation").getOrCreate()
    val sc = spark.sparkContext
    // 获取Streaming实时对象
    val ssc = new StreamingContext(sc,Seconds(2))

    // 1、Streaming-Sources输入
    // 1.1、Text文件输入
    val ds:DStream[String] = ssc.textFileStream("")
    // 1.2、消费Kafka数据 见StreamingKafka单例类

    // 2、Streaming常用Transformation算子
    /*
    2.1、map 转换算子
    2.2、flatMap 扁平化
    2.3、filter 过滤
    2.4、repartition 指定分区数/并行度
    2.5、union 并集
    2.6、count RDD元素计数
    2.7、updateStateByKey 状态流，根据key累加历史数据
    2.8、countByValue key对应value的次数
    2.9、reduceByKey 根据key分组，组内聚合
    2.10、reduce 聚合
    2.11、transform 在DStream上执行任意RDD操作
    */
    // 2.12、window 窗口
    // 由于StreamingContext的批间隔是在ssc创建时设置的，整个任务中所有的流都使用该ssc固定的批间隔
    // 为方便开发者随时可以修改批间隔，SparkStreaming提供了window操作
    // def window(
    // windowDuration: Duration,  窗口大小
    // slideDuration: Duration    窗口滑动步长
    // ): DStream[T] = {}
    // 间隔为5s的窗口
    ds.window(Seconds(5),Seconds(5))
    ds.window(Seconds(5))

    // 3、Streaming-outputOperator输出
    // 3.1、print
    // 3.2、foreachRDD

  }

}
