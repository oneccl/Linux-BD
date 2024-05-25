package com.cc.day0313

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/13
 * Time: 15:26
 * Description:
 */
object DataSetOperation {

  // Flink-Batch批处理转换算子(Transformation)、Sink输出算子

  def main(args: Array[String]): Unit = {
    // 获取Flink-Batch批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 获取批处理对象：DataSet
    val ds:DataSet[String] = env.fromCollection(List("Tom,18","Jack,17","Bob,20"))
    val ds1:DataSet[(String,Int)] = env.fromElements(("a", 1), ("b", 3), ("a", 2))
    val ds2:DataSet[(String,Int)] = env.fromElements(("b", 2), ("c", 4))

    // A、批处理常用Transform算子

    // 1、map 转换
    ds.map(s=>{
      val arr = s.split(",")
      (arr(0),arr(1))
    })//.print()

    // 2、flatMap 扁平化
    ds.flatMap(_.split(","))//.print()

    // 3、filter 过滤
    ds1.filter(t=>t._2%2!=0)//.print()

    // 4、reduce 规约聚合
    ds1.reduce((t1,t2)=>("sum",t1._2+t2._2))//.print()

    // 5、groupBy、reduceGroup 先分区/分组、再组内聚合
    ds1.groupBy(0).reduceGroup(ls=>ls.reduce((t1,t2)=>(t1._1,t1._2+t2._2)))//.print()

    // 6、groupBy、sortGroup 分区内排序取前n个
    ds1.groupBy(0).sortGroup(1,Order.DESCENDING).first(2)//.print()

    // 7、max、min 最大值、最小值、sum
    ds1.groupBy(0).max(1)//.print()

    // 8、aggregate 预聚合：求最大值、最小值、sum（只能作用于元组）
    ds1.groupBy(0).aggregate(Aggregations.MAX,1)//.print()

    // 9、distinct 去重
    ds1.distinct(0)//.print()

    // 10、first 取前n个
    ds1.first(2)//.print()

    // 11、join where equalTo 相同字段的交集
    ds1.join(ds2).where(0).equalTo(0)
      .apply((l,r)=>(l._1,l._2,r._2))//.print()

    // 12、leftOuterJoin/rightOuterJoin 左连接/右连接
    ds1.leftOuterJoin(ds2).where(0).equalTo(0).apply((l,r)=>{
      if (r==null) (l._1,l._2,"default") else (l._1,l._2,r._2)
    })//.print()

    // 13、cross 笛卡尔积(两两组合)
    ds1.cross(ds2)//.print()

    // 14、union 并集
    ds1.union(ds2)//.print()

    // B、批处理Sink输出算子
    // 1）writeAsText、writeAsCsv 写出到本地/hdfs
    // 2）collect 收集

    // 15、setParallelism 设置分区数/并发度（默认4）
    // partitionByHash 按指定Key的Hash分区
    ds1.partitionByHash(0).setParallelism(2)
      .writeAsText("C:\\Users\\cc\\Desktop\\FlinkPart",WriteMode.OVERWRITE)
    //env.execute()

    // 16、sortPartition 指定字段分区内排序
    val ls = ds1.setParallelism(2).sortPartition(1, Order.DESCENDING).collect()
    //println(ls)

  }

}
