package com.cc.day0201

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/1
 * Time: 16:47
 * Description:
 */

object AnimalTest {

  def main(args: Array[String]): Unit = {
    val cat = new Cat
    println(cat.category)
    cat.sound()
    println(cat.eat("鱼"))

    println("--------------")
    val dog = new Dog
    println(dog.category)
    dog.sound()
    println(dog.eat("骨头"))

  }

}
