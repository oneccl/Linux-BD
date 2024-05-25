package com.cc.day0202

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/3
 * Time: 16:29
 * Description:
 */
object DemoFor {

  // 2）for推导式
  def main(args: Array[String]): Unit = {
    // (1) 生成器(Generator): <- .. to ..
    val arr = Array(2, 3, 5, 7)
    // 方式1：使用数组下标数列
    for (e <- 0 to arr.length-1){
      print(e+"\t")
    }
    // 简写
    for (e <- arr.indices){   // arr.indices: 获取数组下标数列
      print(e+"\t")
    }
    // 方式2：for推导式
    for (elem <- arr) {
      print(elem+"\t")
    }
    // (2) 守卫(Guard): 对生成器生成的元素进行过滤，使用;隔开，写在后面
    for (i <- 1 to 10; if i%3 == 0){
      print(i+"\t")  // 打印3的倍数
    }
    // (3) 枚举器(Enumerators): for推导式允许同时包含多个枚举器
    // 相当于for的嵌套，运行时会按照顺序在第一个枚举器取一次值时，执行完第二个枚举器的所有取值
    println("\n------------------")
    // 例：9*9乘法表
    for (i <- 1 to 9; j <- 1 to i){
      printf(s"${j}x${i}=${j*i}\t")  // Scala格式化输出
      if (i==j) println()
    }
    // (3) for-yield: 将生成器中满足守卫条件的元素代入yield后面的表达式运算后将结果收集到集合返回
    // 例：获取字符串中所有字符的ASCII值
    var str = "abc"  // 字符串可作为字符数组遍历
    val list = for (c <- str) yield c.asInstanceOf[Int]
    println(list)  // Vector(97, 98, 99)
  }

}
