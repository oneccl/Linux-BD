package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 9:35
 * Description:
 */

object HelloWord {
  // main()方法：Unit ==> () 无返回值
  def main(args: Array[String]): Unit = {
    println("HelloWord")
    // 定义变量
    var a = 4
    a += 1
    // 对象.方法名(参数)
    a = a.+(1)
    println("a = " + a)
    // 定义常量
    val b = 5
    //b += 1
    println("b = " + b)
    // 方法调用
    println("求和: "+add(a, b))
  }

  // 方法定义
  def add(x:Int,y:Int):Int = {
    return x+y
  }

}



