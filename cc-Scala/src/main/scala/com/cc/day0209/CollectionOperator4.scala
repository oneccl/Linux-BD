package com.cc.day0209

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/9
 * Time: 17:34
 * Description:
 */
object CollectionOperator4 {

  // Scala集合算子（四）

  def main(args: Array[String]): Unit = {
    // 6、reduce、reduceLeft
    // 1)声明：def reduceLeft[B >: A](op: (B, A) => B): B = {}
    // 2)参数：op:二元函数；op参数B,A:从左到右B,A操作数运算后将(上一次)结果/返回值
    //   赋值给第一个操作数B，取集合的下一个数作为第二个操作数A，直到取出所有元素
    // 3)返回值：op的返回值类型B
    // 4)作用：将集合从左到右进行聚合
    val list = List(2, 3, 4, -5, 6)
    val res = list.reduce(_ + _)  // 等同list.sum
    println(res)
    val res1 = list.reduce(_ - _)
    println(res1)
    // reduceRight
    // def reduceRight[B >: A](op: (A, B) => B): B = {}
    val res2 = list.reduceRight(_ + _)  // 等同list.sum
    println(res2)
    val res3 = list.reduceRight(_ - _)
    println(res3)

    println("---------------")

    // 7、fold、foldLeft
    // 1)声明：def foldLeft[B](z: B)(op: (B, A) => B): B = {}
    // 2)参数：初始值B; op:二元函数，op参数：初始值B和集合元素A
    // 3)返回值：op的返回值类型B
    // 4)作用：根据初始值将集合从左到右进行聚合
    val res4 = list.fold(100)(_ - _)  // 相当于100-list.sum
    println(res4)
    // foldRight
    // def foldRight[B](z: B)(op: (A, B) => B): B = {}
    val res5 = list.foldRight(100)(_ - _)
    println(res5)

    println("---------------")

    // 8、zip、zipAll
    // 拉链操作：将两个集合元素一一对应转换为原集合类型[元组]类型
    // zipAll：可设置默认值补充元素数量少的一方
    val list1 = List("a", "b", "c")
    val list2 = List(2, 3)
    val tuples1 = list1.zip(list)
    println(tuples1)
    val tuples2 = list.zip(list1)
    println(tuples2)
    val tuples3 = list1.zipAll(list,"list1默认值","list默认值")
    println(tuples3)
    val tuples4 = list1.zipAll(list2, "list1默认值", "list2默认值")
    println(tuples4)

    println("---------------")

    // 9、mkString
    // 集合元素转字符串并拼接
    val str = list2.mkString  // List(2, 3) ==> 23
    println(str)
    // 参数1：前缀 参数2：用什么分割 参数3：后缀
    val str1 = list2.mkString("[", ",", "]") // List(2, 3) ==> [2,3]
    println(str1)
    // 集合元素转字符串，以什分割
    val str2 = list2.mkString(":")  // List(2, 3) ==> 2:3
    println(str2)


    // 线程安全的集合使用

    // Scala线程安全的集合都已标注过期，推荐使用Java的线程安全的集合
    // 1)Scala线程安全的集合使用：new 实现类 with Scala线程安全的集合接口(trait)
    val map1 = new mutable.HashMap[String, Int]() with mutable.SynchronizedMap[String, Int]
    // 2)使用Java的线程安全的集合
    val map2 = new ConcurrentHashMap[String, Int]()

  }

}
