package com.cc.day0210

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/10
 * Time: 17:26
 * Description:
 */
sealed class SealedClass {

  // 密封类（Sealed class）

  /*
  特质（trait）和类（class）可以用sealed关键字修饰，标记为密封的
  密封类的所有子类都必须与之定义在相同文件中，从而保证所有子类型都是已知的
  密封类可以避免进行不必要的其他情况的匹配
  */
  // 本文件内可访问，其它文件中无法访问

}
