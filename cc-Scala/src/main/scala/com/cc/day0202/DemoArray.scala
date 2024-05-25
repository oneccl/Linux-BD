package com.cc.day0202

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/4
 * Time: 15:37
 * Description:
 */
object DemoArray {

  // Scala集合:

  // 支持可变集合和不可变集合
  // 不可变集合：scala.collection.immutable
  // 可变集合：scala.collection.mutable
  // 主要分为三大类：序列(Seq)、集(Set)、映射(Map)

  def main(args: Array[String]): Unit = {
    // 序列（Seq）

    // 1、定长数组
    val arr1 = new Array[Int](5)  // 创建
    val arr2 = Array(12, 52, 78)
    // 添加/修改元素
    arr1(1) = 10
    arr1.update(3,15)
    // 删除元素：定长数组不能删除元素
    println(arr2(1))  // 取值
    // 显示数组内容（同Java）
    println(util.Arrays.toString(arr1))
    // 数组大小
    println(arr1.length)
    println(arr1.size)
    // 遍历
    for (elem <- arr1) {  // for推导式
      print(elem+"\t")
    }
    for (elem <- arr1.indices) {
      print(elem+"\t")  // 按照索引序列遍历
    }
    // foreach遍历
//    arr2.foreach(e=>print(e+"\t"))
//    arr2.foreach(println(_))  // 一元匿名函数(参数只能使用一次)
//    arr2.foreach(println _)  // 中缀表达式
//    arr2.foreach(println)

    println("\n------------------")

    // 2、可变数组(集合类型)
    val ints1 = new ArrayBuffer[Int]()  // 创建
    val ints2 = ArrayBuffer(22, 65, 40)
    // 添加/修改元素
    ints1.append(11,13,17,23,29)
    ints1.+=(31)  // 添加元素31
    ints2(1) = 66  // 空集合不能使用（索引越界）
    // 插入
    ints2.insert(2,44)  // 指定索引处插入元素（该索引必须存在，否则索引越界）
    // 删除元素
    ints2.remove(2)  // 删除指定索引元素（该索引必须存在，否则索引越界）
    ints2.remove(1,2)  // 删除索引1开始(包括1)后面的2个元素 范围：(m,n)==>[index=m,index+n]
    ints1.-=(23)  // 删除元素23
    // 取值
    println(ints1(3))
    // 集合元素顺序反转输出
    println(ints1.reverse)
    // 显示集合内容
    println(ints1)  // 集合可直接打印显示
    println(ints2)
    // 集合大小
    println(ints1.length)
    println(ints1.size)
    // 遍历
    for (elem <- ints1) {
      print(elem+" ")
    }
    ints1.foreach(println)
    ints1.map(e=>print(e+"\t"))  // map算子
    ints1.map(println)

    println("\n------------------")

    // 3、数组(Array)与集合(ArrayBuffer)互转
    val buffer = arr1.toBuffer  // 数组 --> 集合
    val array = ints1.toArray  // 集合 --> 数组
    println(buffer)
    println(util.Arrays.toString(array))

    println("\n------------------")

    // 4、多维数组（2维）
    // 定义
    val arr2D = Array.ofDim[Int](3, 4)  // 3x4
    arr2D(1)(1)= 99  // 赋值
    // 遍历
    for (row <- arr2D) {  // for推导式
      for (col <- row) {  // 每行是一个数组
        print(col+"\t")
      }
      println()
    }
    println("*************")
    // foreach遍历
    arr2D.foreach(row=>{row.foreach(e=>print(e+"\t"));println()})

    println("------------------")

    // 5、Scala和Java数组/List互操作
    // Scala数组是通过Java实现的
    val list = new util.ArrayList[String]()
    list.add("abc")
    list.add("eq")
    // 需要导包：import scala.collection.convert._  Scala中_为通配符，相当于*
    val scalaList = list.asScala  // Java转Scala
    println(scalaList)
    val javaList = scalaList.asJava  // Scala转Java
    println(javaList)

    println("------------------")

    // 6、例：将数组array的各元素平方后输出
    val res = for (e <- array) yield Math.pow(e, 2)  // 收集
    println(util.Arrays.toString(res))

    array.foreach(e=>println(Math.pow(e,2)))  // 遍历
    array.foreach(e=>print(pow2(e)+"\t"))  // 调用函数遍历
  }

  // 定义平方函数
  var pow2 = n => {Math.pow(n,2)}

}
