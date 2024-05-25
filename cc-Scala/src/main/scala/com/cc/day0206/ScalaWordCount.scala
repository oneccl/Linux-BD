package com.cc.day0206

import scala.collection.mutable
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 17:04
 * Description:
 */
object ScalaWordCount {

  // 单词统计（Scala写法）

  def main(args: Array[String]): Unit = {
    // 读取文件
    val source = Source.fromFile("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt")
    val list = source.getLines().toList
    val hashMap = new mutable.HashMap[String, Int]()
    // 清洗过滤
    // flatMap(): 将List[所有行]展开为List[所有单词]
    list.flatMap(line => line.toLowerCase().replaceAll("\\W+"," ").split("\\s+"))
      // 过滤：每个单词去空格后不为空
      .filter(word => word.trim.nonEmpty)  // 简化 .filter(_.trim.nonEmpty)
      // 每个单词统计1次（word,1）...
      .map(word => (word,1))  // 简化 .map((_,1))
      // 聚合计算
      .map(entry => hashMap.put(entry._1,hashMap.getOrElseUpdate(entry._1,0)+1))
    // 结果输出
    hashMap.toList
      .sortBy(entry => entry._2)  // 排序（根据V升序）
      .reverse  // 反转：降序
      .foreach(println)
  }

}
