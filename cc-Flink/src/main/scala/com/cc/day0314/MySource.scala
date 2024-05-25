package com.cc.day0314

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 17:46
 * Description:
 */

// 自定义Source，用于测试，继承SourceFunction，重写2个方法
// SourceFunction[T]泛型 => 返回类型DataStream[T]
class MySource extends SourceFunction[(String,Double)]{

  private var category:List[String] = List("女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公")
  private var random = Random
  private var flag = false

  // 运行
  override def run(ctx: SourceFunction.SourceContext[(String, Double)]): Unit = {
    while (!flag) {
      val ctg:String = category(random.nextInt(category.length))  // 随机种类
      val price:Double = random.nextDouble() * 100  // 随机价格（100内）
      // 输出数据 (类别,价格)
      ctx.collect((ctg, price))
      Thread.sleep(20)
    }
  }

  // 取消运行
  override def cancel(): Unit = {
    this.flag=true
  }

}
