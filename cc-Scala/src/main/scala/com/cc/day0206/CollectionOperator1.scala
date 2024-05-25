package com.cc.day0206

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/6
 * Time: 18:35
 * Description:
 */
object CollectionOperator1 {

  // Scala集合算子（一）

  def main(args: Array[String]): Unit = {
    val list = List(7, 5, 17, 3, 7, 11, -13, 2)
    println(list.head)  // 获取头元素
    println(list.last)  // 获取尾元素
    println(list.tail)  // 剔除头元素
    println(list.init)  // 剔除尾元素
    println(list.length)  // 集合大小
    println(list.size)  // 集合大小
    println(list.toSet)  // 转为Set去重
    println(list.distinct)  // 去重
    println(list.isEmpty)  // 是否为空
    println(list.nonEmpty)  // 是否不为空
    println(list.sum)  // 求和
    println(list.product)  // 求乘积
    println(list.max)  // 最大值
    println(list.min)  // 最小值
    println(list.reverse)  // 反转、逆序
    println(list.count(_ % 2 != 0))  // 7 满足条件的元素个数
    println(list.forall(_ > 0))  // false 元素是否全部符合条件
    println(list.exists(_ < 0))  // true 是否存在符合条件的元素
    println(list.filter(_ > 0))  // 过滤：得到满足条件的元素
    println(list.partition(_ > 0))  // 将源集合按条件分区，并装入元组返回
    println(list.splitAt(3))  // 按指定索引分区，并装入元组返回
    println(list.take(3))  // 取前3个元素
    println(list.takeRight(3))  // 取后3个元素
    println(list.takeWhile(_ > 0))  // 从左到右寻找满足条件的元素，直到一个元素不满足条件终止
    println(list.drop(3))  // 剔除前3个元素
    println(list.dropRight(3))  // 剔除后3个元素
    println(list.dropWhile(_ > 0))  // 从左到右寻找满足条件的元素，直到一个元素不满足条件终止，剔除满足条件的元素
    println(list.span(_ > 0))  // 从左到右寻找满足条件的元素，直到一个元素不满足条件终止，从该不满足条件的元素处分区，并装入元组返回
  }

}
