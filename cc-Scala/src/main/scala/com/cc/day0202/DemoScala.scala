package com.cc.day0202

import java.io.FileReader
import java.util

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/3
 * Time: 13:55
 * Description:
 */
object DemoScala {
  // Scala: Scalable Language(可扩展语言)
  /*
  Scala是一门现代的多范式语言，它平滑地集成了面向对象和函数式语言的特性
  1、特点
  1）纯面向对象的：一切(包括值)都是对象
  2）函数式的：一切函数都有值
  3）变量类型静态的：变量类型不可改变，编译器通过值自动推断
  4）可扩展的：支持通过隐式类(implicit class)给已有的类型添加扩展方法
  5）互操作性：Scala设计目标是与Java运行环境（JRE）进行良好的互操作
  2、Scala编译
  Main.scala --> 编译器(scalac) --> Main.class --> 运行于JVM
   */
  def main(args: Array[String]): Unit = {
    // 3、数据类型：所有值都有类型
    // 值类型(AnyVal基本类型)：Byte,Short,Int,Long,Float,Double,Boolean,Char,Unit(同Void)
    // 引用类型(AnyRef)：相当于Java中数组、类、接口
    // 超类(Any)：相当于Java中Object
    val a:Long = 123456789
    val b:Float = a  // 小转大
    println(b)   // 1.23456792E8 丢失精度
    val c:Int = a.toInt  // 强转
    val d = a.asInstanceOf[Int] // 强转
    println(c)
    println(d)
    // 4、操作符和中缀表达式
    // 操作符：和Java不同的是，Scala操作符都是方法
    // 中缀表达式：对象.方法名(参数) ==> 对象 方法名 参数
    val x = 2.+(3)
    println(x)
    val y = 2 + 3
    println(y)
    // 5、apply()和update():Scala中提供了很多隐式操作用来简化编码的书写量
    // apply()方法见：object DateUtil静态类
    // update()方法：
    // 数组定义：类型Int,长度5的数组
    val ints = new Array[Int](5)
    ints(2) = 8  // 将索引为2的元素更新为8
    ints.update(0,10)  // 将索引为0的元素更新为10
    println(util.Arrays.toString(ints))  // 查看数组内容
    // 遍历
    ints.foreach(i=>println(i))
    ints.foreach(println(_))  // _ :通配符，只能使用一次
    ints.foreach(println)
    // 6、异常：与Java类似的，Scala提供异常处理机制用于终止错误的程序运行和错误提示
    // Scala没有"受检"异常（受检异常：编译器自动检查，如调用一个抛出了异常的方法
    // 需要在当前方法抛出其异常，或使用try-catch-finally）
//    var reader: FileReader = null
//    try {
//      reader = new FileReader("path")  // Scala不需要抛出,但会打印异常栈
//    } catch {
//      case e:Exception => println(e.getMessage)  // 处理异常
//    } finally {
//      reader.close()
//    }
    println("--------------------")
    // 7、三大结构(顺序、选择、循环)
    // 见day0201包object Demo静态类
    // 1）while()、do-while()补充
    // 例1：打印20以内的斐波那契数列
    var first = 0
    var second = 1
    var next = 0
    while (next < 20){
      print(next+"\t")
      next = first+second
      first = second
      second = next
    }
    // 例2：计算一个正整数每位之和
    var n = 258
    var sum = 0
    do {
      var every = n%10
      sum += every
      n /= 10
    } while (n != 0)
    println("\n"+sum)
    // Scala中没有break和continue关键字，可以使用return、条件控制或BreaksAPI
    // 例3：寻找大于m的第一个质数
    var m:Int = 32
    while (m >= 32){
      var count:Int = 0
      for (i:Int <- 1 to m/2){
        if (m % i == 0){
          count += 1
        }
      }
      if (count == 1){
        println(m)
        return   // m置为<31的一个数，while循环也结束，如m=0
      }
      m += 1
    }
    // 2）for补充：for推导式
    // 见object DemoFor静态类
  }

}
