package com.cc.day0201


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 16:02
 * Description:
 */

// 类(class)
class Student {

  var id:Int = 0
  var name: String = ""
  var gender:String = ""
  var age:Int = 0
  var score:Double = 0

  def eat(food:String): String ={
    // this：当前类的对象的引用
    this.name+"今天中午吃的"+food
  }

  def sleep(time:Double): String ={
    this.name+"昨晚睡了"+time+"小时"
  }

  // override：方法重写，不可省略
//  override def toString:String ={
//    "{"+id+","+name+","+gender+","+age+","+score+"}"
//  }

  override def toString = s"Student($id, $name, $gender, $age, $score)"

}
