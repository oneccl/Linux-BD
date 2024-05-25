package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 16:18
 * Description:
 */

object StudentTest {

  def main(args: Array[String]): Unit = {
    // 创建对象，()可省略
    val student = new Student
    // 对象初始化
    student.name="小张"
    student.id=12345678
    student.age=18
    student.gender="男"
    student.score=88.8
    // 对象.方法
    println(student.eat("泡馍"))
    println(student.sleep(8.5))
    println(student.toString)
  }

}
