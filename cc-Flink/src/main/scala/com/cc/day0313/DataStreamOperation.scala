package com.cc.day0313

import org.apache.flink.streaming.api.scala._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 9:08
 * Description:
 */
object DataStreamOperation {

  // Flink-Streaming实时处理转换算子(Transformation)

  def main(args: Array[String]): Unit = {
    // 获取Flink-Streaming实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取实时处理对象DataStream
    val ds:DataStream[String] = env.readTextFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    //ds.print()

    // 1、Streaming实时处理转换算子

    // 统计每个用户的数据总和（code=200）
    // 1.1、map 转换
    val ds1:DataStream[(String,LogDetail)] = ds.map(line => {  // 清洗
      val arr = line.split("\\s+")
      val ip = arr.head
      val dt = arr(3).substring(1, 15)
      val code = arr(8)
      val upload = try arr(9).toLong catch {case e: Exception => 0L}
      val download = try arr(arr.length - 1).toLong catch {case e: Exception => 0L}
      (ip, LogDetail(ip, dt, code, upload, download))
    })
    // 1.2、filter 过滤
    val ds2:DataStream[(String,LogDetail)] = ds1
      .filter(_._2.code == "200")
      // 1.3、keyBy 按键分区：将相同Key字段的分配到同一分区(分组)
      .keyBy(_._1)
      // 1.4、reduce 规约聚合
      .reduce((t1, t2) => (t1._1, t1._2.sum(t2._2)))
    ds2.print()

    val ds3 = env.fromElements("a,1","b,3","c,2")
    // 1.5、flatMap 扁平化
    ds3.flatMap(_.split(","))//.print()

    // 物理分区：
    // 1.6、shuffle 随机分区：将数据随机打乱，对同组数据，每次得到的结果都不同
    // 1.7、rebalance 轮循分区：将数据随机打乱，随机轮循写入下游分区
    // 1.8、partitionCustom 自定义分区

    // 1.9、关于窗口的算子见FlinkWindowOperation和FlinkWindowFuncs单例类

    // 开启任务：执行实时任务
    env.execute("DataStream")

  }

}

// 案例类
case class LogDetail(var ip:String,
                     var dt:String,
                     var code:String,
                     var upload:Long,
                     var download:Long
                    ){

  def sum(o:LogDetail):LogDetail={
    this.ip=o.ip
    this.dt=o.dt
    this.code=o.code
    this.upload+=o.upload
    this.download+=o.download
    this
  }

}