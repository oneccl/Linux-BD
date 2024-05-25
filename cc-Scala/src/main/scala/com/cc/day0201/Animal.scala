package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 16:32
 * Description:
 */

// 特质(trait)：类似Java中interface，允许多继承
trait Animal {

  val category:String = "动物"

  // 动物叫声
  def sound()

  // 动物食物
  def eat(food:String):String

}

// 猫
class Cat extends Animal {
  // override：方法重写，可省略
  override def sound(): Unit = {
    println("喵喵喵！")
  }

  override def eat(food: String): String = {
    "猫吃"+food
  }

}

// 狗
class Dog extends Animal {

  override def sound(): Unit = {
    println("旺旺旺！")
  }

  override def eat(food: String): String = {
    "狗吃"+food
  }

}
