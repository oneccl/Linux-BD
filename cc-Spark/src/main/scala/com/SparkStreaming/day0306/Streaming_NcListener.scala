package com.SparkStreaming.day0306

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/6
 * Time: 14:54
 * Description:
 */
object Streaming_NcListener {

  // nc -l -k port 网络控制监听

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NcListener")
    // 批处理时间间隔 2s获取一次数据（Duration的单例类成员）
    val duration = Seconds(2)
    // 获取Spark实时处理对象
    // def this(conf: SparkConf, batchDuration: Duration) = {}
    val ssc = new StreamingContext(conf,duration)
    // 输入：通过socket加载控制台输入数据
    // bd91开启一个网络端口: nc -l -k 4567
    val dStream:DStream[String] = ssc.socketTextStream("bd91", 4567)
    // 处理、计算、输出
    dStream.print()  // 打印结果

    // 启动流
    ssc.start()
    // 设置流的状态为等待关闭
    ssc.awaitTermination()

  }

}
