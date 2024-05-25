package com.cc.day0310

import org.apache.flink.streaming.api.scala._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/11
 * Time: 19:17
 * Description:
 */
object SocketWordCount {

  // StreamingJob Flink实时处理

  def main(args: Array[String]): Unit = {
    // 1、获取Flink实时处理执行环境：StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2、输入：通过socket监听主机端口获得输入
    // 获取Flink实时处理对象：DataStream
    // bd91开启一个监听端口：nc -lk 8888
    val ds:DataStream[String] = env.socketTextStream("bd91", 8888)
    // 3、数据处理
    val ds1:DataStream[(String,Int)] = ds
      .flatMap(_.split("\\s+"))
      .map((_,1))
      // 通过第1个字段分组
      .keyBy(_._1)
      // 聚合第1个字段
      .sum(1)
    // 4、输出
    ds1.print()
    // 5、执行：开启任务
    env.execute("Flink SocketWordCount")
  }

}
