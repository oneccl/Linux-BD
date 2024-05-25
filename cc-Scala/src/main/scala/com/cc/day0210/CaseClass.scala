package com.cc.day0210

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/10
 * Time: 15:47
 * Description:
 */
object CaseClasses {

  // 案例类/样例类（Case class）

  def main(args: Array[String]): Unit = {
    // 创建
    val s1 = new Student("Tom", 18)
    val s2 = Student("Jack", 18)
    s1.age = 19  // 属性修改
    println(s1.age)  // 属性访问
    println(s2)  // 对象打印
    val s3 = Student("Tom", 19)
    // 案例类在使用==比较时是按照对应值比较的，而非按照引用
    println(s1 == s3)  // true 可以使用==比较
    println(s1.equals(s3))  // true
  }

}

// 定义：case class 类名([属性列表]) [{}]  其中[]中内容可省略
case class Student(val name:String,var age:Int){
  // 案例类的属性默认使用val修饰，非常适用于不可变的数据
  // 如果需要使变量是可变的，可以使用var修饰
  // 内含apply()伴生方法
}
