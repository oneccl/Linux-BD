package com.cc.day0206

import scala.collection.mutable
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 16:23
 * Description:
 */
object WordCount {

  // 单词统计（类Java写法）

  def main(args: Array[String]): Unit = {
    // 读取文件
    val source = Source.fromFile("C:\\Users\\cc\\Desktop\\temp\\HarryPotter.txt")
    // 获取所有行
    val lines = source.getLines()
    source.close()
    val hashMap = new mutable.HashMap[String, Int]()
    // 清洗过滤
    for (line <- lines) {
      val words = line.toLowerCase.replaceAll("\\W+"," ").split("\\s+")
      for (word <- words){
        val w = word.trim
        if (w.nonEmpty){
//          if (!hashMap.contains(w)){
//            hashMap.put(w,1)
//          } else {
//            hashMap.put(w,hashMap(w)+1)
//          }
          hashMap.put(w,hashMap.getOrElseUpdate(w,0)+1)
        }
      }
    }
    // 结果输出
    hashMap.toList  // Map不能排序，需要转换为List排序
      .sortBy(entry => entry._2)  // 根据元组的Value排序（默认升序）
      .reverse  // 反转：降序
      .foreach(println)  // 打印
  }

}
