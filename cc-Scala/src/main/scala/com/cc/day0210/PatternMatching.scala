package com.cc.day0210

import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/10
 * Time: 16:08
 * Description:
 */
object PatternMatching {

  // 模式匹配（Pattern matching）
  // match-case语句

  def main(args: Array[String]): Unit = {
    // 语法：变量 match {至少一个case语句}

    // 1、值匹配
    val n = Random.nextInt(5)
    println("n = "+n)
    // 生成：n.match 类似switch-case匹配
    val res = n match {
      case 0 => "Zero"
      case 1 => "One"
      case 2 => "Two"
      case _ => "Other"  // 相当于switch-case中的default
    }
    println("res = "+res)

    println("-------------")

    // 2、类型匹配
    matchOnType(10)
    matchOnType(2.78)
    matchOnType("abc")
    matchOnType(true)

    println("-------------")

    // 3、模式守卫
    patternGuards(88.5)
    patternGuards(75)
    patternGuards(59)

  }

  def matchOnType(n:Any): Unit ={
    // match-case语句可以直接匹配对象的真实类型
    n match {
      case x:Int => println(s"${x} is a Int")
      case x:String => println(s"${x} is a String")
      case x:Double => println(s"${x} is a Double")
      case _ => println(s"${n} is a Any")
    }
  }

  def patternGuards(n:Double): Unit ={
    // match-case语句可以在变量后添加类似for推导式中的守卫进行匹配
    n match {
      case x if x>=85 => println(s"${x} : A")
      case x if x>=75 => println(s"${x} : B")
      case x if x>=60 => println(s"${x} : C")
      case _ => println(s"${n} : D")
    }
  }

}
