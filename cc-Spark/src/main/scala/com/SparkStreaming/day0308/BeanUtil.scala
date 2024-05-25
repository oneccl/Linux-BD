package com.SparkStreaming.day0308

import java.lang.reflect.{Field, Modifier}
import scala.util.control.Breaks

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/26
 * Time: 19:48
 * Description:
 */
object BeanUtil {

  // 维度关联对象属性拷贝

  // 将srcObj中属性的值拷贝到destObj对应的属性上（属性名相同）
  def copyFields(srcObj:AnyRef,destObj:AnyRef): Unit ={
    if (srcObj==null || destObj==null){
      return
    }
    // 获取srcObj的所有属性
    val srcFields:Array[Field] = srcObj.getClass.getDeclaredFields
    // 处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable{
        // get/set: Scala会自动为类中的属性提供get/set方法
        // get: fieldName()
        // set: fieldName_$eq(参数类型)

        // get 方法名
        val getMethodName = srcField.getName
        // set 方法名
        val setMethodName = srcField.getName + "_$eq"

        // get 方法对象 从srcObj中获取get方法对象
        val srcGetMethod = srcObj.getClass.getDeclaredMethod(getMethodName)
        // set 方法对象 从destObj中获取set方法对象
        val destSetMethod =
        try {
          destObj.getClass.getDeclaredMethod(setMethodName,srcField.getType)
        }catch {
          // NoSuchMethodException 抛出异常，跳出本次循环
          // Breaks.break()：抛出异常，会跳出整个循环，需要Breaks.breakable{}捕获，不会导致整个循环结束
          case e:Exception => Breaks.break()
        }

        // 忽略val/final属性：对val修饰的属性不处理
        val destField = destObj.getClass.getDeclaredField(srcField.getName)
        // destField.getModifiers 获取修饰符
        if (destField.getModifiers.equals(Modifier.FINAL)){
          Breaks.break()
        }

        // 调用get方法获取srcObj的属性值，调用destObj的set方法赋值
        val srcFieldValue = srcGetMethod.invoke(srcObj)
        destSetMethod.invoke(destObj,srcFieldValue)
      }
    }
  }

}
