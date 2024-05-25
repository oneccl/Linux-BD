package com.cc.day0206

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 15:29
 * Description:
 */
object ScalaCollection2 {

  def main(args: Array[String]): Unit = {
    // 4、队列（Queue）先进先出
    val queue = mutable.Queue[Int]()  // 创建
    // 入列
    queue.enqueue(2,3)
    queue+=(5,7,11)
    // 出列
    queue.dequeue()
    println(queue.front)  // 获取队列头元素
    println(queue.head)  // 获取头元素
    println(queue.last)  // 获取尾元素
    println(queue.init)  // 剔除尾元素
    println(queue.tail)  // 剔除头元素
    println(queue)  // 打印

    println("-----------------")

    // 5、栈（Stack）先进后出
    val stack = mutable.Stack[Int]()  // 创建
    // 进栈、压栈
    stack.push(2,4,6,8,10)
    // 出栈、弹栈
    stack.pop()
    println(stack.top)  // 获取栈顶元素
    println(stack.head)  // 获取栈头元素（栈顶元素）
    println(stack.last)  // 获取栈底元素
    println(stack.init)  // 剔除栈底元素
    println(stack.tail)  // 剔除栈顶元素
    println(stack)  // 打印

    println("-----------------")

    // 6、列表（List）
    // 声明列表，并添加元素
    val list1 = List(5, "a", true)
    // 声明空列表
    val list2 = List()
    val list3 = Nil
    // 头部添加元素，形成新List
    val list4 = 20 +: list1
    // 尾部添加元素，形成新List
    val list5 = list1 :+ 10
    println(list4)
    println(list5)
    // 拼接多个List内容
    val list6 = list2 ++ list3 ++ list4 ++ list5
    println(list6)
    // 将多个元素拼接到List头部 e1 :: e2 :: ... :: List
    val list7 = list2 :: list3 :: list4 :: list5
    println(list7)
    // 列表大小
    println(list1.length)
    println(list1.size)
    // 获取前两个元素
    println(list1.take(2))
    // 遍历
    for (elem <- list1) {
      println(elem)
    }
    list1.foreach(println)
  }

}
