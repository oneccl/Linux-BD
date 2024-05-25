package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 11:00
 * Description:
 */

object Demo {

  def main(args: Array[String]): Unit = {
    // 1、选择/分支：if语句
    var score = 80
    // 写法1：
    if (score > 60){
      println("及格了!")
    }else {
      println("再接再厉!")
    }
    // 写法2：
    if (score > 60) println("及格了!") else println("再接再厉!")
    // 写法3：
    var res = if (score > 60) "及格了!" else "再接再厉!"
    println(res)

    println("----------------")
    // 2、循环：for语句
    // 鸡兔同笼: 鸡:x，兔:y
    // 头15个，腿42只
    // x+y=15
    // 2x+4y=42
    for (x:Int <- 0 to 15){
      val y = 15 - x
      if (2*x+4*y==42){
        println("鸡: "+x+"\t兔: "+y)
      }
    }

    println("----------------")
    // 3、循环：while语句
    // 喝饮料：共50瓶，喝3换1，最多可喝多少瓶
    // Java可以使用for循环计算；Scala使用for循环无法计算
//    var sum = 50
//    for (i:Int <- 1 to sum){
//      if (i % 3 == 0){
//        sum += 1
//      }
//    }
//    println("最多: "+sum)

    var sum = 50
    var num = 1  // 从第一瓶开始
    while (num <= sum){
      if (num%3 == 0){
        sum += 1
      }
      num += 1
    }
    println("最多: "+sum)

    println("----------------")
    // 代码块：最后一行为返回结果，可用变量接收
    val block = {
      val a = 1
      a + 1
    }
    println(block)

    println("----------------")
    // 方法调用：
    println("相反数："+opposite(10))
    println("默认值相加："+add())
    println("相加："+add(2,3))
    println("填入第一个参数相加："+add(2))
    println("填入第二个参数相加："+add(y=3))
    println("相减："+subtract(2)(3))
    println("当前主机用户："+getUserName)

    println("----------------")
    // 函数调用：
    println("相乘："+multiply(4, 5))
    println("绝对值："+abs(10))
  }

  // 方法定义：def 方法名(参数列表): 返回值类型 = {方法体}

  // 方法体最后一行为返回结果，return可省略
  def opposite(n:Int): Int ={
    return -n
  }
  // 方法形参可赋默认值
  def add(x:Int=1,y:Int=1): Int ={
    x+y
  }
  // 方法的多个形参可分开写
  def subtract(x:Int)(y:Int): Int ={
    x-y
  }
  // 方法无参时，方法后面的()可省略
  // 方法体只有一条语句时，{}可省略
  // 获取当前主机用户
  def getUserName: String = System.getProperty("user.name")

  // 函数(匿名)定义：(形参列表) => {函数体}

  // 可用一个变量接收函数的结果
  // 当函数体只有一条语句时，{}可省略
  val multiply = (m: Int, n: Int) => m * n
  val abs = (n:Int) => if (n<0) -n else n

}
