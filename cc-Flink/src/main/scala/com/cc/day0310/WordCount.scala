package com.cc.day0310

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/11
 * Time: 19:32
 * Description:
 */
object WordCount {

  // BatchJob Flink批处理

  def main(args: Array[String]): Unit = {
    // 1、获取Flink批处理执行环境：ExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2、输入：通过读取text文件获得输入
    // 获取Flink批处理对象：DataSet
    val ds:DataSet[String] = env.readTextFile("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt")
    // 3、数据处理
    val ds1:DataSet[(String,Int)] = ds
      .flatMap(_.toLowerCase
        .replaceAll("\\W+"," ")
        .split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // 通过第1个字段分组
      .groupBy(0)
      // 分组内聚合，聚合第2个字段
      .reduceGroup(it=>it.reduce((v1,v2)=>(v1._1,v1._2+v2._2)))
      // 根据第2个字段排序
      .sortPartition(1,Order.DESCENDING)
    // 4、输出
    ds1.print()

    // 5、执行：批处理不需要该语句
    //env.execute("Flink WordCount")

  }

}
