package com.cc.day0202

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/3
 * Time: 8:47
 * Description:
 */

object DateUtil {

  def main(args: Array[String]): Unit = {
    // 获取起始年份间的所有闰年
    println(getLeapYears(1900, 2023))
    // 判断该年份是否是闰年
    println(isLeapYear(2023))
    // 时间戳格式化
    println(dateFormat(0L, "yyyy-MM-dd hh:mm:ss"))
    println(dateFormat(System.currentTimeMillis(), "yyyy-MM-dd"))
    // 使用伴生方法时间戳格式化
    println(apply(0L, "yyyy-MM-dd hh:mm:ss"))
    println(DateUtil(System.currentTimeMillis(), "yyyy-MM-dd"))
  }

  /*
  闰年分为普通闰年和世纪闰年，其判断方法为：公历年份是4的倍数，且不是100的倍数，为普通闰年
  公历年份是整百数，且必须是400的倍数才是世纪闰年；总结：四年一闰；百年不闰，四百年再闰
  */
  // 获取起始年份间的所有闰年
  def getLeapYears(sta:Int,end:Int): String ={
    var res = ""
    var count = 0
    for (year:Int <- sta to end){
      if (year%4 == 0 && year%100 != 0 || year%400 == 0){
        count += 1
        if (count%2 == 0){
          res += year+"\n"
        } else {
          res += year+"\t"
        }
      }
    }
    return res
  }

  // 判断该年份是否是闰年
  def isLeapYear(year:Int): Boolean ={
    if (year%4 == 0 && year%100 != 0 || year%400 == 0) true else false
  }

  // 时间戳格式化
  def dateFormat(stamp:Long, pattern:String):String ={
    println("自定义方法!")
    val T = new SimpleDateFormat(pattern)
    T.format(new Date(stamp))
  }

  // 单例类的伴生方法，相当于Java构造方法
  def apply(stamp: Long, pattern: String): String = {
    println("伴生方法!")
    val T = new SimpleDateFormat(pattern)
    T.format(new Date(stamp))
  }

}
