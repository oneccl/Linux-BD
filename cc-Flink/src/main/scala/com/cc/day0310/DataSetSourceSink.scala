package com.cc.day0310

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/11
 * Time: 19:52
 * Description:
 */
object DataSetSourceSink {

  // Flink-Batch批处理输入输出

  def main(args: Array[String]): Unit = {
    // 获取Flink-Batch批处理执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 1、Flink-Batch-Source批处理 输入

    // 1.1、Text文件/目录输入（可指定编码）
    val ds1 = env.readTextFile("","UTF-8")
    // 1.2、元素输入
    val ds2 = env.fromElements("a", "b", "c")
    // 1.3、集合输入
    val ds3 = env.fromCollection(List(("a", 1), ("b", 2)))
    val ds4 = env.fromCollection(1 to 10)
    val ds5 = env.fromCollection(Iterator("a","b","c"))

    // 2、Flink-Batch-Sink批处理 输出
    // 见DataSetOperation单例类

  }

}
