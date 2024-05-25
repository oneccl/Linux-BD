package com.SparkStreaming.day0308

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/8
 * Time: 20:05
 * Description:
 */
object DouYinArea {

  // 业务2：实时统计抖音用户热度地区(城市)榜前10
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DouYinArea").master("local[2]").getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\StreamingDouYinArea")

    // 获取Streaming实时处理对象
    val ssc = new StreamingContext(sc, Seconds(2))
    // Kafka拉取数据配置
    val confMap = Map(
      ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
      ("bootstrap.servers", "bd91:9092,bd92:9092,bd93:9092"),
      ("group.id", "g66"),
      ("auto.offset.reset", "earliest")
    )
    // 获取kafka操作对象
    val ds: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,           // 轮循获取
      ConsumerStrategies.Subscribe[String, String](  // 订阅主题，获取数据
        Iterable("douyin_digg"),
        confMap
      )
    )

    // 数据计算，获取ds对象
    val ds1: DStream[((String, String), UserInfo)] = ds.map(_.value())
      // 数据清洗
      .map(line => {
        val cols = line.split("\t")
        val userId = cols(10)
        val city = cols(1)
        val digg = cols(7).toInt
        val nickName = cols(12)
        val workId = cols(0)
        val comment = cols(8).toInt
        val share = cols(9).toInt
        // 对象类型处理
        ((city, userId), UserInfo(userId, nickName, city, workId, digg, comment, share))
      })
      // 根据city和userId分组并聚合
      .reduceByKey((u1, u2) => u1.sum(u2))  // 单批次结果
      .updateStateByKey((vs: Seq[UserInfo], sr: Option[UserInfo]) => {  // 累加历史结果
        if (vs.nonEmpty) {                  // 当前批次存在历史数据的Key
          val valuesSum: UserInfo = vs.reduce((u1, u2) => u1.sum(u2))   // 当前批次聚合
          sr match {
            case None => Option(valuesSum)
            case _ => Option(valuesSum.sum(sr.get))  // 当前批次累加历史结果
          }
        } else sr
      })

    // 数据计算，并保存到数据库
    val prop = new Properties()  // 数据库连接属性
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")

    ds1.foreachRDD((rdd: RDD[((String, String), UserInfo)]) => {
      val list:List[UserInfo] =
        rdd                             // RDD[(String, Iterable[UserInfo])]
        .repartition(1)    // 单分区内操作
        .map(_._2)                      // 获取UserInfo
        .groupBy(_.city)                // 根据city分组
        .map(_._2.toList)               // RDD[List[UserInfo]]
        .map(ls => {
          ls.sortBy(_.digg)             // 根据digg排序（升序）
            .takeRight(10)              // 从右拿前10
            .reverse                    // 返回ls:List[UserInfo]
        })
        .collect()
        .toList
        .flatten                        // flatMap扁平化
      // 前3原始结果：List(List(UserInfo(98775105323,妞笑个,西安,6680124060208237838,4758,4731,0), UserInfo(110320941658,用户7611429175467,西安,6678886486802255112,2625,2554,35), UserInfo(100250414747,瞎扯淡,西安,6678880412191739147,296,290,0)), List(UserInfo(88085842188,媛儿,安康,6679711559767411981,936,900,0), UserInfo(109883437546,佳,安康,6680089497637555469,644,630,0)), List(UserInfo(62724133026,往事随风而去，有关必回……,宿州,6679725327201176835,2450,2297,0), UserInfo(66354396587,没有公主命，要有女王范,宿州,6679735529954725123,448,435,0), UserInfo(101384643778,李姐姐,宿州,6680109273537694983,315,308,0)), List(UserInfo(61519673596,媛儿,徐州,6679707439937375495,6237,5940,0), UserInfo(106156612228,🍎一生平安🍎,徐州,6679714921728511243,2340,2302,0), UserInfo(65243972076,一路格桑花红袖添香,徐州,6680484338758438148,720,703,0)), List(UserInfo(96405996654,终究是个谜❤️,绍兴,6678176555388210435,28223,26159,13299), UserInfo(86903817706,以后的以后........,绍兴,6680423271386582284,1092,1080,0), UserInfo(93620883301,兰姐,绍兴,6679226317856115981,810,787,0)), List(UserInfo(99737302547,小草,东营,6680124793661951243,228,224,0), UserInfo(54603827221,A  酷狗宠物用品13589457899,东营,6679719455238425859,130,126,0), UserInfo(100372591353,雪雪（有关必回）,东营,6679717994496544008,116,112,0)), List(UserInfo(71746188691,小优雅,商洛,6679702769428876548,1904,1872,0), UserInfo(58593551535,冰可乐,商洛,6678884911014792462,1280,1253,0), UserInfo(65678508471,女子美容养生会馆,商洛,6679708525754715406,630,589,0)), List(UserInfo(107744047982,初中学习技巧,北京,6678619525858069773,1399610,1358567,147458), UserInfo(71050918348,江河,北京,6673008633267113229,167900,164865,500), UserInfo(72583628848,cana,北京,6678624547895332103,111188,105650,4522)))
      // 前3扁平化结果：List(UserInfo(98775105323,妞笑个,西安,6680124060208237838,4758,4731,0), UserInfo(110320941658,用户7611429175467,西安,6678886486802255112,2625,2554,35), UserInfo(100250414747,瞎扯淡,西安,6678880412191739147,296,290,0), UserInfo(88085842188,媛儿,安康,6679711559767411981,936,900,0), UserInfo(109883437546,佳,安康,6680089497637555469,644,630,0), UserInfo(62724133026,往事随风而去，有关必回……,宿州,6679725327201176835,2450,2297,0), UserInfo(66354396587,没有公主命，要有女王范,宿州,6679735529954725123,448,435,0), UserInfo(101384643778,李姐姐,宿州,6680109273537694983,315,308,0), UserInfo(61519673596,媛儿,徐州,6679707439937375495,6237,5940,0), UserInfo(106156612228,🍎一生平安🍎,徐州,6679714921728511243,2340,2302,0), UserInfo(65243972076,一路格桑花红袖添香,徐州,6680484338758438148,720,703,0), UserInfo(96405996654,终究是个谜❤️,绍兴,6678176555388210435,28223,26159,13299), UserInfo(86903817706,以后的以后........,绍兴,6680423271386582284,1092,1080,0), UserInfo(93620883301,兰姐,绍兴,6679226317856115981,810,787,0), UserInfo(99737302547,小草,东营,6680124793661951243,228,224,0), UserInfo(54603827221,A  酷狗宠物用品13589457899,东营,6679719455238425859,130,126,0), UserInfo(100372591353,雪雪（有关必回）,东营,6679717994496544008,116,112,0), UserInfo(71746188691,小优雅,商洛,6679702769428876548,1904,1872,0), UserInfo(58593551535,冰可乐,商洛,6678884911014792462,1280,1253,0), UserInfo(65678508471,女子美容养生会馆,商洛,6679708525754715406,630,589,0), UserInfo(107744047982,初中学习技巧,北京,6678619525858069773,1399610,1358567,147458), UserInfo(71050918348,江河,北京,6673008633267113229,167900,164865,500), UserInfo(72583628848,cana,北京,6678624547895332103,111188,105650,4522))

      // 需要设置数据库的特殊字符编码格式utf8mb4，否则报错
      // List => RDD => DataFrame DF匹配对象类型
      val df = sc.makeRDD(list).toDF("userId", "nickName", "city", "workId", "digg", "comment", "share")
      df.write.mode(SaveMode.Overwrite).jdbc(
        "jdbc:mysql://localhost:3306/day0307_sparkstreaming?serverTimezone=UTC",
        "area_uw_digg",
        prop
      )
    })

    // 开启ds流
    ssc.start()
    ssc.awaitTermination()
  }

}

// 案例类
case class UserInfo(var userId:String,    // 用户ID
                    var nickName:String,  // 用户昵称
                    var city:String,      // 所在城市
                    var workId:String,    // 作品ID
                    var digg:Int,         // 点赞
                    var comment:Int,      // 评论
                    var share:Int         // 分享
                   ){

  def sum(o:UserInfo): UserInfo ={
    this.userId=o.userId
    this.nickName=o.nickName
    this.city=o.city
    this.workId=o.workId
    this.digg+=o.digg
    this.comment+=o.digg
    this.share+=o.share
    this
  }

}