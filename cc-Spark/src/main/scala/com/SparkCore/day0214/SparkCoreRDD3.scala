package com.SparkCore.day0214

import com.utils.BaiduAipNlp
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/14
 * Time: 19:08
 * Description:
 */

object SparkCoreRDD3 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  // 离线大数据编程思想：
  /*
  1）数据来源：JavaWeb埋点数据（后台日志）
  2）数据采集：flume
  3）数据存储：HDFS
  4）数据计算：Spark、MR
  5）结果保存：数据库
  6）前端显示：BI
  */

  def main(args: Array[String]): Unit = {
    // 3、flatMap

    // 例3：统计大数据岗位描述中技术框架关键字出现的排名
    val conf = new SparkConf().setMaster("local[2]").setAppName("LexicalAnalyzer")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("C:\\Users\\cc\\Desktop\\temp\\spark\\西安大数据.txt")
    rdd.filter(_.nonEmpty)  // 过滤为空的行
      .filter(_.split("\t").length>10)  // 过滤小于11列的行
      .map(_.split("\t")(10))  // 获取第11列（岗位描述）内容
      .repartition(4)  // 设置4个分片(Task)
      // 3.1、flatMap：扁平化为1维
      // def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {}
      // 将返回的List转换为Scala集合，并扁平化为1维
      .flatMap(BaiduAipNlp.parse(_).asScala)
      .filter(_.matches("[a-zA-Z]+"))  // 过滤除了英文的字段（词）
      .map(f=>(f.toLowerCase,1))  // 英文字段转小写计1
      .reduceByKey(_+_)  // 根据K聚合
      .sortBy(_._2,false,1)  // 根据V排序
      .foreach(println)  // 输出
  }

}
