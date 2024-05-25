package com.cc.day0209

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 19:11
 * Description:
 */
object CollectionOperator2 {

  // Scala集合算子（二）

  def main(args: Array[String]): Unit = {
    val list = List(2, 3, 3, 4, 5)
    val array = Array("abc", "void")
    // 1、map
    // 1）声明：def map[B, That](f: A => B): That = {}
    // 2）参数：f:一元函数，f参数B:原集合元素的类型；f返回值A:任意类型
    // 3）返回值：That:原集合类型，该集合中存入f返回值A类型
    // 4）作用：原集合元素与f一一映射，将f返回值收集到新的同原集合类型的集合中
    // map的_ :取的是集合中的每个元素
    val tuples:List[(Int,Int)] = list.map((_, 1))
    println(tuples)
    val ints:List[Double] = list.map(_ * 1.0)
    println(ints)
    val ints1:Array[Int] = array.map(_.length)
    println(ints1.toList)

    println("------------------")

    // 2、flatMap
    // 1）声明：def flatMap[B, That](f: A => GenTraversableOnce[B]): That = {}
    // GenTraversableOnce[B] 集合
    // 2）参数：f是一元函数，f参数B:原集合元素的类型；f返回值A:任意类型
    // 3）返回值：That:原集合类型，该集合中存入f返回值A类型
    // 4）作用：原集合元素与f一一映射，如果f返回值仍是集合类型(包括字符串)
    //   会将f返回值再次扁平化(多层展开)，直到不能扁平化后收集
    // flatMap中的_ :取的第一层，取的是集合中的每个元素
    // 此处_取的是字符串
    val arr = array.flatMap(_.toCharArray)
    println(arr.toList)
    // 此处_取的是字符串
    val strs = array.flatMap(_.split("\\s+"))
    println(strs.toList)
    // 此处x取的是字符串，执行(展开)了2层
    val chars = array.flatMap(x=>x)  // 同array.flatten
    println(chars.toList)
    // 此处_取的是字符串，flatMap(字符串)自动转换为字符数组
    val chars1 = array.flatMap(_.substring(0, 2))
    println(chars1.toList)

    println("------------------")

    // 3、foreach
    // 1）声明：def foreach[U](f: A => U) : Unit = {}
    // 2）参数：f:一元函数，f参数U:原集合元素的类型，f返回值:任意类型
    // 3）返回值：Unit
    // 4）遍历
    list.foreach(println)

    println("------------------")

    // 4、sortBy、reverse、distinct
    // 1）返回值：返回排好序的原集合类型[元素类型]
    // 2）作用：默认从小到大排序
    val list1 = List(5, 10, -3, 7, 2, 5)
    val list2 = list1.sortBy(x => x)  // 升序
    println(list2)
    val list3 = list2.reverse  // 降序
    println(list3)
    val list4 = list1.distinct.sortBy(x=>x).reverse  // 去重后降序
    println(list4)

    println("------------------")

    // 例：统计一个字符串中每个字母出现的次数(不区分大小写)
    var str = "CollectionOperator"
    letterCountFromStr(str)

  }

  def letterCountFromStr(str:String): Unit ={
    val list = List(str)
    val hashMap = new mutable.HashMap[Char, Int]()
    list.flatMap(_.toLowerCase.trim)  // 字符串会被拆为List[字符]
      .map((_,1))
      .foreach(entry => hashMap.put(entry._1,hashMap.getOrElseUpdate(entry._1,0)+1))
    hashMap.toList
      .sortBy(_._2)
      .reverse
      .foreach(println)
  }

}
