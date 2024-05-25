package com.cc.day0316

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/3
 * Time: 18:22
 * Description:
 */
object FlinkBroadcastVar {

  // Flink广播变量
  // 和Spark中的广播变量一样，Flink也支持在各个节点中保存数据，所在的计算节点实例
  // 可在本地内存中直接读取被⼴播的数据，可以避免Shuffle，提高并行效率

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 创建需要⼴播的数据集
    val dataSet1: DataSet[Int] = env.fromElements(1, 2, 3, 4)
    // 创建输⼊数据集
    val dataSet2: DataSet[String] = env.fromElements("Flink", "Spark")

    // 读取广播变量
    dataSet2.map(
      // 使用RichFunction读取广播变量
      new RichMapFunction[String, String]() {
        var broadcastSet: Traversable[Int] = _
        // 获取广播变量数据集，并转换成List
        override def open(config: Configuration): Unit = {
          broadcastSet = getRuntimeContext
            .getBroadcastVariable[Int]("broadcastSet").asScala
        }
        // 输出格式
        override def map(value: String): String = {
          value +"\t"+ broadcastSet.toList
        }
      }
    )
      // 广播DataSet数据集，指定广播变量名称为broadcastSet
      .withBroadcastSet(dataSet1, "broadcastSet")
      .print()
    /*
    Flink	 List(1, 2, 3, 4)
    Spark	 List(1, 2, 3, 4)
    */

  }

}
