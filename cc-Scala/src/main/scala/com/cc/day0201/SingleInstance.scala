package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 15:52
 * Description:
 */

// 对象(object)：用来定义单个实例，可以看做它自己类(单例类/静态类)的单例
// 对象继承App时，就是一个应用，无需再使用main方法
object SingleInstance extends App {

  val name:String = "abc"
  val gender:Char = '男'
  val salary:Double = 1888.8

  override def toString:String ={
    "{"+name+","+gender+","+salary+"}"
  }

  println(SingleInstance.name)
  // this：当前对象
  println(this.salary)
  println(println(toString))

}
