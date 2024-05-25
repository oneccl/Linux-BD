package com.SparkStreaming.day0306

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/6
 * Time: 15:44
 * Description:
 */
object NcL_WordCount {

  // 单词统计

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NcL_WordCount")
    val sc = new SparkContext(conf)
    // 设置检查点结果保存的位置（不设置报错）
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\checkpointDStream")
    // 批处理间隔时间2s
    val duration = Seconds(2)
    // 获取Spark实时处理对象
    // def this(sparkContext: SparkContext, batchDuration: Duration) = {}
    val ssc = new StreamingContext(sc,duration)
    // 输入
    val dStream:DStream[String] = ssc.socketTextStream("bd91", 4567)

    // 处理(仅统计了当前行的单词)
//    dStream.flatMap(_.split("\\s+"))
//      .map((_,1))
//      .reduceByKey(_+_)
//      .print()

    // 处理(统计当前行单词+上次该key数据)
    dStream.flatMap(_.split("\\s+"))
      .map((_,1))
      // updateStateByKey 状态流
      // updateStateByKey: 将上次结果保存到State与本次结果根据key聚合
      // 参数二元函数：Seq[V]: 当前结果 Option[S]: 上次结果
      // def updateStateByKey[S: ClassTag](
      // updateFunc: (Seq[V], Option[S]) => Option[S]
      // ): DStream[(K, S)] =
      .updateStateByKey((values:Seq[Int],stateRes:Option[Int])=>{
        stateRes match {
          case None => Option(values.sum)  // 上次结果为空，仅聚合本次结果
          case _ => Option(stateRes.get+values.sum)  // 上次结果不为空，累加聚合
        }
      })
      .print()

    // 启动ds
    ssc.start()
    ssc.awaitTermination()

  }

}
