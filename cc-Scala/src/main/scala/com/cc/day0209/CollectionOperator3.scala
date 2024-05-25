package com.cc.day0209

import scala.collection.mutable
import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/9
 * Time: 16:22
 * Description:
 */
object CollectionOperator3 {

  // Scala集合算子（三）

  def main(args: Array[String]): Unit = {
    // 5、collect
    // 1)声明：def collect[B, That](pf: PartialFunction[A, B]): That = {}
    // PartialFunction[A, B] 偏函数
    // 2)参数：pf:偏函数
    // 3)返回值：原集合类型
    // 4)作用：按过滤条件分类收集
    // 例：获取日志文件：请求流量  时间小时  响应码
    val source = Source.fromFile("C:\\Users\\cc\\Desktop\\temp\\log-20211101.txt")
    // 该方法返回Iterator[String]类型，需要转换为具体集合才能运算
    val lines = source.getLines().toList
    source.close()
    val iterator = lines.map(line => {
      val fieldArr = line.split(" ")
      val upload = strToLong(fieldArr(9))
      val code = fieldArr(8)
      val hour = fieldArr(3).split(":")(1)
      (upload, hour, code)
    })
//    iterator.foreach(println)

    // 统计3000>=请求流量>1000 按响应码进行计数
    // 统计请求流量>3000的访问时间 按小时进行计数
    val codeMap = new mutable.HashMap[String, Int]()
    val hourMap = new mutable.HashMap[String, Int]()

    // 方式1: 使用collect(偏函数)
//    iterator.collect(new PartialFunction[(Long,String,String),Unit] {
//      // 条件过滤
//      override def isDefinedAt(x: (Long, String, String)): Boolean = {
//        x._1 > 1000
//      }
//      // 聚合统计逻辑
//      override def apply(v1: (Long, String, String)): Unit = {
//        if (v1._1>3000){
//          hourMap.put(v1._2,hourMap.getOrElseUpdate(v1._2,0)+1)
//        } else {
//          codeMap.put(v1._3,codeMap.getOrElseUpdate(v1._3,0)+1)
//        }
//      }
//    })

    // 方式2：使用foreach
//    iterator.foreach(x=>{
//      if (x._1>3000){
//        hourMap.put(x._2, hourMap.getOrElseUpdate(x._2, 0) + 1)
//      } else if (x._1>1000){
//        codeMap.put(x._3,codeMap.getOrElseUpdate(x._3,0)+1)
//      }
//    })

    // 方式3：使用collect的偏函数的简化形式
    // collect使用偏函数进行匹配
    // 输出元素的个数可以与原函数的元素个数不同
    // 只有能被case匹配的对象才完成相应的收集操作
    iterator.collect {
      case x if x._1 > 3000 => hourMap.put(x._2, hourMap.getOrElseUpdate(x._2, 0) + 1)
      case x if x._1 > 1000 => codeMap.put(x._3, codeMap.getOrElseUpdate(x._3, 0) + 1)
    }

    hourMap.foreach(println)
    println("---------------")
    codeMap.foreach(println)

    println("---------------")

    // 例：对集合元素：偶数取相反数，奇数取绝对值
    val list = List(2, 3, 5, -7, 8)
    // 方式1：
    val list1 = list.map(x => {
      if (x % 2 == 0) -x else if (x > 0) x else -x
    })
    println(list1)
    // 方式2：
    val list2 = list.collect{
      case x if x%2==0 => -x
      case x if x%2!=0 => if (x>0) x else -x
    }
    println(list2)

  }

  // 字符串转Long
  def strToLong(s:String):Long ={
    try {
      s.toLong
    } catch {
      case e: Exception =>0L
    }
  }

}
