package com.cc.day0210

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/10
 * Time: 16:37
 * Description:
 */
object CaseClassMatching {

  // 案例类/样例类匹配

  def main(args: Array[String]): Unit = {
    val p1 = new SmartPhone("HUAWEI", "5G")
    val p2 = new SmartPhone("MI", "4G")
    val c1 = new Computer("LENOVO", "512G")
    matchByType(p1)  // 类型匹配
    matchByType(p2)
    matchByType(c1)

    println("----------------")

    caseClassMatch(p1)  // 案例类/样例类匹配
    caseClassMatch(p2)
    caseClassMatch(c1)
  }

  // 类型匹配
  def matchByType(device: Device): Unit ={
    device match {
      case d:SmartPhone => println(s"手机{品牌:${d.brand},网络:${d.netType},功能:${d.extraFunction()}}")
      case d:Computer => println(s"电脑{品牌:${d.brand},磁盘:${d.diskSize},功能:${d.extraFunction()}}")
      case _ => println("其它设备")
    }
  }
  // 案例类/样例类匹配
  def caseClassMatch(device: Device): Unit ={
    device match {
      // 匹配指定属性值
      case SmartPhone("HUAWEI", "5G") => println(s"手机{品牌:HUAWEI,网络:5G,功能:${device.extraFunction()}}")
      // 形参n：匹配任意网络
      case SmartPhone("MI", n) => println(s"手机{品牌:MI,网络:${n},功能:${device.extraFunction()}}")
      // 形参x,y：匹配任意属性值
      case SmartPhone(x, y) => println(s"手机{品牌:${x},网络:${y},功能:${device.extraFunction()}}")
      case Computer(x, y) => println(s"电脑{品牌:${x},磁盘:${y},功能:${device.extraFunction()}}")
    }
  }
}

// 1、定义抽象类(虚基类)
abstract class Device(){
  def extraFunction():Any
}
// 2、定义两个案例类分别继承抽象类
case class SmartPhone(brand:String,netType:String) extends Device{
  override def extraFunction(): String = {
    "打电话"
  }
}
case class Computer(brand:String,diskSize:String) extends Device{
  override def extraFunction(): String = {
    "办公"
  }
}
