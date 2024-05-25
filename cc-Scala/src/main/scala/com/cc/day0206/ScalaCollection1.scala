package com.cc.day0206

import java.util.Date
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 13:59
 * Description:
 */
object ScalaCollection1 {

  def main(args: Array[String]): Unit = {
    // 1、元组(Tuple)
    // 元组是不同数据类型的集合
    val tuple = ("Tom", 18, '男', 1.8, new Date())  // 创建
    println(tuple._1)  // 数据访问，从1开始
    println(tuple _3)  // 中缀：数据访问
    println(tuple.productElement(4))  // 通过下标访问，从0开始
    println(tuple.productArity)  // 获取元祖的长度/大小
    println(tuple)  // 打印
    println(tuple.productIterator.toList)  // 元组转为List
    // 拉链操作(zip):
    var arr1 = Array("a","b","c")
    var arr2 = Array(2,3,5)
    val t3:Array[(String,Int)] = arr1.zip(arr2)  // 数组[元祖]
    println(t3.toList)  // 数组转List

    println("-----------------")

    // 2、集(Set)
    // 集是不重复元素的集合
    val set = new mutable.HashSet[Int]()
    // 添加
    set.add(2)
    set.add(3)
    set.add(3)
    set.add(5)
    set.+(5)
    set+=(6)
    // 删除
    set.remove(5) // 删除元素5
    // 打印
    println(set)
    // 遍历
    for (elem <- set) {  // 推导式
      println(elem)
    }
    set.foreach(println)  // foreach

    println("-----------------")

    // 3、映射(Map)
    // 映射是K-V的集合
    // 创建
    val map1 = mutable.Map("a" -> 10, "b" -> 8)
    val map2 = mutable.Map(("a", 10), ("b", 8))
    val map3 = new mutable.HashMap[String, Int]()  // 空映射
    // 添加值
    map3.put("c",18)
    map3.+=("d"->20)
    map3+=("e"->3)
    // 获取值
    // Scala为单个值提供了对象的包装器Option，其有2个子类：Some:某个值 None:空值
    println(map3.get("d"))  // Some(20)
    println(map3.get("d").get)  // 20
    println(map3("d"))  // 20
    // 修改值
    map3.put("d",30)  // Key相同，则修改
    map3("d") = 40
    map3.update("d",50)
    // 删除值
//    map3.remove("d")
//    map3-=("c")
    val map4 = map3.drop(2)  // 删除前2个，得到一个新map，原map不变
    println(map4)
    // 打印
    println(map1)
    println(map2)
    println(map3)
    // 遍历
    // foreach：根据entry遍历
    map3.foreach(println)  // 打印元祖形式
    // 迭代器：根据entry遍历
    val iterator = map3.iterator
    while (iterator.hasNext){
      println(iterator.next())  // 打印元祖形式
    }
    // 推导式
    for ((k,v) <- map3){
      printf(s"${k} : ${v}\n")
    }
    // 遍历Key
    for (k <- map3.keys) {
      println(k)
    }
    // 遍历Value
    for (v <- map3.values) {
      println(v)
    }
  }

}
