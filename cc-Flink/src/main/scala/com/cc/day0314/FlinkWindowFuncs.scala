package com.cc.day0314

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousEventTimeTrigger, ContinuousProcessingTimeTrigger, CountTrigger, ProcessingTimeTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/15
 * Time: 10:46
 * Description:
 */
object FlinkWindowFuncs {

  // Flink窗口函数、触发器
  // 窗口函数、触发器基于窗口算子

  def main(args: Array[String]): Unit = {
    // 获取实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取实时处理流对象
    val ds:DataStream[(String,Int)] = env.fromElements(("a",1), ("b",3), ("a",2))
    // 窗口算子
    val ws = ds.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))

    // 1、窗口函数

    // A、增量聚合函数
    // 1.1、reduce 规约聚合函数
    ws.reduce((t1,t2)=>(t1._1,t1._2+t2._2))

    // 1.2、aggregate 预聚合函数，可用于计算max、min、sum、count、avg等
    // 自定义累加器：AggregateFunction[IN, ACC, OUT]
    // 自定义窗口输出：WindowFunction[IN, OUT, KEY, W <: Window]
    // // def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
    //      preAggregator: AggregateFunction[T, ACC, V],
    //      windowFunction: (K, W, Iterable[V], Collector[R]) => Unit): DataStream[R] = {}
    // 使用见Double11Case单例类

    // B、全量聚合函数
    // 1.3、process 全窗口函数
    // 自定义处理结果输出：ProcessWindowFunction[IN, OUT, KEY, W <: Window]
    // def process[R: TypeInformation](
    //      function: ProcessWindowFunction[T, R, K, W]): DataStream[R] = {}
    // 使用见Double11Case单例类

    // 2、触发器（Trigger）
    // trigger 触发执行窗口函数
    // 使用见Double11Case单例类

    // 2.1、时间周期性触发：每隔2s触发
    ws.trigger(ContinuousProcessingTimeTrigger.of[TimeWindow](Time.seconds(2)))
    // 2.2、计数触发：每隔2个元素触发
    ws.trigger(CountTrigger.of[TimeWindow](2))
    // 2.3、时间周期性事件触发(默认)
    ws.trigger(ContinuousEventTimeTrigger.of[TimeWindow](Time.seconds(2)))

  }

}
