package com.cc.day0314

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/15
 * Time: 8:50
 * Description:
 */
object FlinkWindowOperation {

  // Flink 时间与窗口
  // Flink窗口算子

  /*
  1、时间：Flink是一个分布式处理系统，各节点相互独立，互不影响，对于流数据，没有全局统一的时钟
  2、水位线(事件时间)：不依赖系统时间，自定义逻辑时钟，每个并行子任务依靠该逻辑时钟执行
  3、水位线特点：
    1）水位线是基于数据的时间戳生成的，水位线就是数据流中的标记
    2）水位线的时间戳必须单调递增，以保证任务的实践时钟一直向前推进
  4、窗口：无界流的分割单位，将数据切割成有限大小的存储桶；对当前窗口数据进行同内计算、输出
  5、窗口分类：
    1）时间窗口（TimeWindow）
    2）计数窗口（CountWindow）
  6、窗口数据分配器（Window Assigner）
    1）滚动窗口（TumblingWindow）：固定大小，均匀分片，首尾连接，没有重叠
    2）滑动窗口（SlidingWindow）：固定大小，设置滑动步长，非首尾连接，重叠
    3）全局窗口（GlobalWindows）：将相同key的所有数据分配到同一个窗口
  7、窗口的生命周期
    1）窗口的创建：由数据驱动创建：当第一个数据到达窗口时，就会创建窗口
    2）窗口计算的触发：窗口分配器=>触发器=>执行窗口函数
    3）窗口的销毁：Flink只对时间窗口进行销毁，计数窗口基于全局窗口实现，而全局窗口不会清除状态，所以就不会销毁
  */

  def main(args: Array[String]): Unit = {
    // 获取实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取实时处理流对象
    val ds:DataStream[(String,Int)] = env.fromElements(("a",1), ("b",3), ("a",2))

    // Flink窗口算子

    // 1、窗口API：时间窗口

    // 1.1、按键分区窗口
    // ds.keyBy(key selector)
    //   .window(window assigner)
    //   .窗口函数/窗口算子

    // 1.1.1、滚动窗口
    ds.keyBy(_._1)  // 开启窗口时间为：北京时间早上8点
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
    ds.keyBy(_._1)
      // 东八区(UTC+8)，设置偏移量-8h，使开启窗口时间为：北京时间00:00:01
      // 参数1(size): 窗口大小 参数2(offset): 偏移量
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2),Time.hours(-8)))

    // 1.1.2、滑动窗口
    ds.keyBy(_._1)
      // 参数1(size): 窗口大小 参数2(slide): 滑动步长
      .window(SlidingProcessingTimeWindows.of(Time.seconds(2),Time.seconds(1)))

    // 1.1.3、全局窗口
    ds.keyBy(_._1)
      .window(GlobalWindows.create())

    // 1.2、非按键分区
    // ds.windowAll(window assigner)

    // 2、窗口API：计数窗口
    // 2.1、滚动窗口: size:窗口大小，当元素数量达到size时触发执行计算并关闭窗口
    // 2.2、滑动窗口: 每个窗口统计size个元素，每隔slide个数据触发计算
    // ds.keyBey(key selector)
    //   .countWindow(size,slide)
    ds.keyBy(_._1)
      .countWindow(2)

  }

}
