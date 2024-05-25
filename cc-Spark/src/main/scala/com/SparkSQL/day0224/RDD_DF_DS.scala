package com.SparkSQL.day0224

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/25
 * Time: 17:08
 * Description:
 */
object RDD_DF_DS {

  // RDD、DataFrame、Dataset互转
  // 3者包含关系: (DS (DF (RDD)))
  // DataFrame、Dataset底层结构实现

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("RDD_DF_DS").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.makeRDD(List(("Tom", 18, 1.80), ("Jack", 20, 1.75)))
    // *需要导入隐式转换包
    import spark.implicits._

    // 1、RDD => DF（List可直接toDF）

    val df:DataFrame = rdd.toDF()
    df.show()
    /*
       _1| _2|  _3|
    | Tom| 18| 1.8|
    |Jack| 20|1.75|
    */
    // 指定列名
    val df1 = rdd.toDF("name", "age", "high")
    df1.show()
    /*
    |name|age|high|
    | Tom| 18| 1.8|
    |Jack| 20|1.75|
    */

    // 2、DF => RDD

    // 创建DataFrame对象：
    // def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = withActive {}
    // 1)需要一个RDD[Row]
    // trait Row extends scala.Serializable
    // class GenericRow(values : scala.Array[scala.Any]) extends Row
    // -- GenericRow：仅保存了每行的数据，没有保存结构信息
    // class GenericRowWithSchema(values : scala.Array[scala.Any], schema : StructType) extends GenericRow
    // -- GenericRowWithSchema：既保存了每行的数据，又保存了结构信息
    // 2)需要一个StructType
    // case class StructType(fields : scala.Array[StructField]) extends spark.sql.types.DataType
    // 3)需要一个StructField
    // case class StructField(name : String, dataType : DataType)
    // 4)需要一个DataType
    // abstract class DataType() extends spark.sql.types.AbstractDataType

    // A、DataFrame: 底层使用RDD[Row]存储了以行为对象的数据，使用StructType存储了表结构
    // Row中使用Array[Any]保存每行的所有列数据，取出元素时需要类型判断和强转，存在类型安全问题
    // RDD仅保存了数据，DF相较于RDD还保存了表结构信息(字段名，字段类型)

    val rdd1:RDD[Row] = df1.rdd
    val df2 = spark.createDataFrame(
      rdd1: RDD[Row],  // RDD[Row]: 存储数据（基于内存）
      StructType(      // StructType: 存储表结构
        Array(StructField("name", StringType), // Array[StructField]: 存储表字段及类型
          StructField("age", IntegerType))
      )
    )
    df2.show()
    /*
    |name|age|
    | Tom| 18|
    |Jack| 20|
    */
    val rdd2 = df2.rdd
    rdd2.map(r=>{
      // Row类型元素访问3种方式：Any => 具体类型
      val name1 = r.getString(0)  // 访问指定类型
      val name2 = r.get(0).asInstanceOf[String] // 强转(不转不会报错)
      val name3 = r.getAs[String](0)  // 指定类型访问
      (name1,name2,name3)
    }).foreach(println)

    // 3、RDD => DS

    val ds:Dataset[(String,Int,Double)] = rdd.toDS()
    ds.show()    // 不能指定列名
    /*
    |  _1| _2|  _3|
    | Tom| 18| 1.8|
    |Jack| 20|1.75|
    */

    // 4、DS => RDD
    val rdd3:RDD[(String,Int,Double)] = ds.rdd
    // 元组泛型 => (案例类)对象泛型
    val ds1:Dataset[Stu] = rdd3
      .map(t => Stu(t._1, t._2, t._3))
      .toDS()
    ds1.show()

    // B、Dataset: DataFrame = Dataset[Row]

    // Row子类实现了存储行数据和表结构信息
    // DS会自动根据对象结构，使用对象属性为列名，属性类型为列字段类型，不存在类型安全问题
    /*
    |name|age|High|
    | Tom| 18| 1.8|
    |Jack| 20|1.75|
    */

    // 5、DF => DS
    // DF存储了RDD[Row]的行对象及表结构
    // DS使用Row实现了行数据及表结构存储
    val ds2:Dataset[Row] = df
    ds2.show()

    // 6、DS => DF
    val df3:DataFrame = ds1.toDF()
    df3.show()
    // Dataset[Row] = Dataset[Stu].toDF()
    // DS=>DF本质是将[案例类]解析为[Row]
    val ds3:Dataset[Row] = ds1.toDF()

  }

}

case class Stu(var name:String, var age:Int, var High:Double)