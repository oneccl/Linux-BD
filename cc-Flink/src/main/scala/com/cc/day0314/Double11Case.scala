package com.cc.day0314

import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.DecimalFormat
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 17:34
 * Description:
 */

object Double11Case {

  // Flink案例：业务需求：
  // 1、实时统计当天0点到当前时间的销售总额（11-11 00:00:00~11-11 23:59:59）
  // 2、统计销售额排名前3的类别
  // 3、每秒更新一次结果

  def main(args: Array[String]): Unit = {
    // 获取实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置执行模式（Batch、Streaming(默认)、AutoMatic）
    // 新版本：流批一体，默认为流处理模式，可手动设置模式
    // AutoMatic自动模式：程序根据输入的数据源是否有界，自动选择执行模式
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    // Source输入: 自定义数据源，模拟实时产生数据，获取实时流对象
    val ds:DataStream[(String,Double)] = env.addSource(new MySource)
    //ds.print()  // (类别,价格)

    // 数据处理
    val ds1:DataStream[CategoryInfo] = ds
      .keyBy(_._1)  // 按类别分组/分区
      // 滚动时间窗 按天滚动
      //.window(TumblingProcessingTimeWindows.of(Time.days(1)))
      // 滑动时间窗 每秒滑动一次
      //.window(SlidingProcessingTimeWindows.of(Time.days(1),Time.seconds(1)))
      // 滚动时间窗 设置偏移量-8: 东八区，从00:00:01开始按天滚动
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      // 周期触发器：在该时间窗口(指定TimeWindow)中，每隔1秒执行一次
      .trigger(ContinuousProcessingTimeTrigger.of[TimeWindow](Time.seconds(1)))
      // 聚合第2个字段
      // sum: 底层调用了aggregate(AggregationType.SUM,pos)
      //.sum(1)
      // aggregate: 预聚合：只能作用于元组
      // def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
      //      preAggregator: AggregateFunction[T, ACC, V],
      //      windowFunction: (K, W, Iterable[V], Collector[R]) => Unit): DataStream[R] = {}
      // 自定义累加器，自定义窗口输出形式
      .aggregate(new MyAggregate,new MyWindow)
    // 预聚合输出
    //ds1.print()

    // Sink输出: 实时统计所有类别销售总额及销售额排名前3的类别
    ds1.keyBy(_.dt)  // 按时间分组，每秒统计一次
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      // process：自定义处理结果输出
      // def process[R: TypeInformation](
      //      function: ProcessWindowFunction[T, R, K, W]): DataStream[R] = {}
      .process(new MyProcess)

    // 懒执行：触发任务/事件执行
    env.execute("Double11Case")

  }

}

// 自定义累加器：[IN, ACC, OUT]泛型 => [输入类型,累加器类型,输出类型]
class MyAggregate extends AggregateFunction[(String,Double),Double,Double]{
  // 累加器初始值
  override def createAccumulator(): Double = {
    0d
  }
  // 当前值与累加器聚合
  override def add(value: (String, Double), acc: Double): Double = {
    value._2+acc
  }
  // 返回累加器
  override def getResult(acc: Double): Double = {
    acc
  }
  // 合并多个累加器结果
  override def merge(acc1: Double, acc2: Double): Double = {
    acc1+acc2
  }
}

// 自定义窗口输出形式：[IN, OUT, KEY, W <: Window]泛型 => [输入，输出，分组Key，窗口]
class MyWindow extends WindowFunction[Double,CategoryInfo,String,TimeWindow]{
  // TimeWindow=1s，根据key分组，input=1s内所有类别的值集合(总销售额)，out=自定义CategoryInfo输出形式
  override def apply(key: String, window: TimeWindow, input: Iterable[Double], out: Collector[CategoryInfo]): Unit = {
    val ctgTotal = input.iterator.next()
    val dt = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss").format(new Date)
    out.collect(CategoryInfo(key,ctgTotal,dt))
  }
}

// 案例类
case class CategoryInfo(var ctg:String,
                        var ctgTotal:Double,
                        var dt:String   // 当前时间，以s为单位
                       ) {

  def sum(o:CategoryInfo):CategoryInfo={
    this.ctg=o.ctg
    this.ctgTotal+=o.ctgTotal
    this.dt=o.dt
    this
  }

}

// 自定义处理业务输出结果：[IN, OUT, KEY, W <: Window]泛型 => [输入，输出，分组Key，窗口]
class MyProcess extends ProcessWindowFunction[CategoryInfo,Object,String,TimeWindow]{
  // key 时间，elements：当前时间窗口的所有类别信息
  override def process(key: String, context: Context, elements: Iterable[CategoryInfo], out: Collector[Object]): Unit = {
    // 1、获取总销售额
    var total:Double = 0d
    elements.foreach(o=>total+=o.ctgTotal)  // 累加
    // 2、获取销售额排名前3的类别
    val list:List[CategoryInfo] = elements.toList.sortBy(_.ctgTotal).reverse.take(3)
    // 小数格式化输出 保留2位小数
    val totalFormat = new DecimalFormat("0.00").format(total)
    // 3、结果显示
    println("---------------------------------")
    println("时间: "+key.split(" ")(1))
    println("成交额: "+totalFormat)
    println("排名\t\t类别\t\t销售额")
    var count = 0
    list.foreach(o => {
      val ctg = o.ctg
      val ctgTotalFormat = new DecimalFormat("0.00").format(o.ctgTotal)
      count+=1
      println(count+ "\t\t" + ctg +"\t\t" + ctgTotalFormat)
    })
  }
}
