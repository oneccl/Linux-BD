package com.SparkCore.day0216

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/2/16
 * Time: 14:44
 * Description:
 */
object SparkCoreRDD8 {

  // （二）RDD转换算子（transformation）
  // RDD.转换算子 => RDD

  def main(args: Array[String]): Unit = {
    // 8、aggregateByKey：指定每个分区的起始值进行V聚合
    // join（key交集）、full（key并集）、left、right
    // cogroup：多维度统计

    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("demo4"))
    val rdd = sc.makeRDD(1 to 10)

    rdd.map(n=> if (n%2!=0) (n+1,1) else (n,1))
      // 8.1、aggregateByKey: 与combineByKey语法相似
      // def aggregateByKey[U: ClassTag](
      //      zeroValue: U          每个分区的起始值(一般为0)
      //      )(
      //      seqOp: (U, V) => U,   将所有V聚合结果再与初始值zeroValue聚合（每个分区）
      //      combOp: (U, U) => U   将多个分区的结果进行合并
      //      ): RDD[(K, U)] = {}
      .aggregateByKey(0)(
        (v1:Int,v2:Int)=>v1+v2,
        (c1:Int,c2:Int)=>c1+c2
      )
      //.foreach(println)

    val rdd1 = sc.makeRDD(List("k1" -> 1, "k2" -> 2))
    val rdd2 = sc.makeRDD(List("k2" -> 1, "k3" -> 2))
    // 8.2、join：(Key交集,(每个Key对应的所有V))
    // def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {}
    // 返回两个RDD中共同的K的V集合
    val rdd3 = rdd1.join(rdd2)
    println(rdd3.collect().toList)

    // 8.3、left（左连接）
    // def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] =
    // 以左RDD(主)为基准，显示左RDD所有内容，右RDD(从)没有对应key的显示为None
    // 有对应key的显示为Some(V)，右RDD的V都为Option类型
    val rdd4 = rdd1.leftOuterJoin(rdd2)
    println(rdd4.collect().toList)

    // 8.4、right（右连接）
    // def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] =
    // 以右RDD(主)为基准，显示右RDD所有内容，左RDD(从)没有对应key的显示为None
    // 有对应key的显示为Some(V)，左RDD的V都为Option类型
    val rdd5 = rdd1.rightOuterJoin(rdd2)
    println(rdd5.collect().toList)

    // 8.5、full：(Key并集,(每个Key对应的所有V))
    val rdd6 = rdd1.fullOuterJoin(rdd2)
    println(rdd6.collect().toList)

    println("---------------")

    // join、left、right、full底层都调用了cogroup

    // 例10：陕西(rdd7)和四川(rdd8)多城市华为和小米手机的销售情况分别为
    val rdd7 = sc.makeRDD(List("HUAWEI" -> 8754, "MI" -> 7688, "HUAWEI" -> 5688, "MI" -> 6152, "MI" -> 5978))
    val rdd8 = sc.makeRDD(List("HUAWEI" -> 6768, "HUAWEI" -> 5890, "HUAWEI" -> 7216, "MI" -> 6880))
    // 8.6、cogroup: 多维度统计
    // 将两个RDD按照相同的K分组，并将每个K的所有V以Iterable集合显示
    // def cogroup[W](
    //      other: RDD[(K, W)]
    //      ): RDD[(K, (Iterable[V], Iterable[W]))] = {}
    val rdd9 = rdd7.cogroup(rdd8)
    rdd9.foreach(println) // (MI,(CompactBuffer(7688, 6152, 5978),CompactBuffer(6880)))
                          // (HUAWEI,(CompactBuffer(8754, 5688),CompactBuffer(6768, 5890, 7216)))

    // 1)统计华为和小米分别在每个省份的销售量
    rdd9.map(t=>{
      (t._1,t._2._1.sum,t._2._2.sum)
    }).foreach(println)   // (MI,19818,6880)
                          // (HUAWEI,14442,19874)
    // 2)统计华为和小米的总销售量
    rdd9.map(t=>{
      val sum = t._2._1.sum + t._2._2.sum
      (t._1,sum)
    }).foreach(println)   // (MI,26698)
                          // (HUAWEI,34316)
  }

}
