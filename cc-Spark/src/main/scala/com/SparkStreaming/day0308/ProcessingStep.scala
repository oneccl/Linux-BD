package com.SparkStreaming.day0308

import com.day0228.HBaseCrudUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jettison.json.JSONObject

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try


/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/12
 * Time: 11:18
 * Description:
 */
// 数据处理流程（Kafka(DWD)->）
/*
1）准备实时环境
2）从Redis中读取offset
3）指定offset从Kafka消费数据
4）提取消费到的数据的offset
5）处理数据：结构转化、去重、维度关联
6）写入下游(Kafka、ES(可自动去重)、HBase等)
7）保存/提交offset到Redis
*/
object ProcessingStep {

  def main(args: Array[String]): Unit = {
    // 1、准备实时环境
    val conf = new SparkConf().setAppName("ProcessingStep").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 2、从Redis中读取offset
    val topic:String = "Topic"
    val groupId:String = "GroupId"
    val offsets:Map[TopicPartition,Long] = OffsetManager.getOffset(topic, groupId)

    // 3、从Kafka消费数据
    var ds:DStream[ConsumerRecord[String,String]] = null
    if (offsets != null && offsets.nonEmpty){
      // 指定offset消费
      ds = KafkaUtil.getKafkaDStream(ssc,Array(topic),groupId,offsets)
    } else {
      // 使用Kafka默认offset消费
      ds = KafkaUtil.getKafkaDStream(ssc,Array(topic),groupId)
    }

    // 4、提取offset结束点
    var offsetRanges:Array[OffsetRange] = null
    val ds1 = ds.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    // 5、处理数据，得到宽表
    // 转化：输出转化：对象->默认保存为json字符串；读取转化：json->对象
    /*
    val ds2 = ds1.map(record => {
      val value = record.value()
      val jsonObj:JSONObject = JSON.parseObject(value)
      jsonObj
    })
    */
    // 去重: 如日活：对同一用户该天的所有操作仅统计一行得到日活宽表，使用Redis第三方审查
    // 日活宽表：日活统计可以基于uId(用户id)，也可基于mId(设备id)
    // 方案1：filter 根据mId过滤  对每条数据进行判断过滤，每条数据都需要连接Redis
    //ds2.filter(o=>o.mId == null)
    // 方案2：mapPartitions过滤  一个分区开关一次Redis
    /*
    ds2.mapPartitions(iter=>{
      // 存储过滤后的数据
      val pageLogs = ListBuffer()
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val jedis = RedisUtil.getJedisFromPool
      // 提取每条数据中的mId
      for (pageLog <- iter) {
        val mId = pageLog.mId
        val date:String = sdf.format(new Date())
        val dauKey = s"DAU:$date"
        // 取出Redis中的所有mId
        val mIds = jedis.smembers(dauKey)
        // 判断Redis是否包含
        // 方式1：存在分布式环境下高并发问题，数据可能会重复
        if (!mIds.contains(mId)){
          jedis.sadd(dauKey,mId)
          pageLogs.append(pageLog)
        }
        // 方式2：优化 Redis是单线程的，可以通过返回值使判断和写入实现了原子性操作
        // 返回值：若返回1：表示添加成功 若Redis已存在，则返回0，不允许再添加
        val isNew:Long = jedis.sadd(dauKey, mId)
        if (isNew == 1L){
          pageLogs.append(pageLog)
        }
      }
      // 一个分区开关一次
      jedis.close()
      // 返回过滤结果
      pageLogs.iterator
    })
    */
    // 维度关联
    // 日活宽表（页面访问信息、用户信息、地区信息、日期时间）
    // 过程：查询Redis，使用mapPartitions进行转化（内部将多个维度表json类型转化为对象）
    // 方法1：将PageLog中的每个字段值挨个提取，赋值给DauInfo中对应的字段
    // 方法2：优化：创建对象属性拷贝工具类，直接完成（见BeanUtil.scala单例类）
    // BeanUtil.copyFields(pageLog,dauInfo)
    /*
    // 页面数据关联业务维度数据
    val uId:String = pageLog.user_id
    // Redis维度key：DIM:表名:ID
    // 1）关联用户维度
    val redisUIdKey:String = s"DIM:USER_INFO:$uId"
    val userInfoJson:String = jedis.get(redisUIdKey)
    val userInfoJsonObj:JSONObject = JSON.parseObject(userInfoJson)
    // 根据技术文档提取需要属性
    // 性别
    val gender:String = userInfoJsonObj.getString("gender")
    // 根据出生日期换算年龄
    val birthday:String = userInfoJsonObj.getString("birthday")
    val birthdayLD:LocalDate = LocalDate.parse(birthday)
    val nowLD:LocalDate = LocalDate.now()
    val period:Period = Period.between(birthdayLD,nowLD)
    val age:Int = period.getYears
    // 补充到对象中
    dauInfo.user_gender = gender
    dauInfo.user_age = age
    // 2）关联地区维度
    */

    // 双流join
    // 订单宽表（订单信息事实表OrderInfo + 订单详情事实表OrderDetail）
    // 内外连接使用需要将流转化为(K,V)的DStream类型，K作为连接条件
    // 1）内连接 join 交集
    // 2）外连接
    // 左外连接：leftOuterJoin 主表(左表全部)+从表(右表匹配)
    // 右外连接：rightOuterJoin 主表(右表全部)+从表(左表匹配)
    // 全外连接：fullOuterJoin 左表全部+右表全部
    /*
    val orderInfoKVDStream:DStream[(Long,OrderInfo)] =
        orderInfoDStream.map(orderInfo=>(orderInfo.id,OrderInfo))
    val orderDetailKVDStream:DStream[(Long,OrderDetail)] =
        orderDetailDStream.map(orderDetail=>(orderDetail.order_id,OrderDetail))
    val orderJoinDStream:DStream[(Long,(OrderInfo,OrderDetail))] =
        orderInfoKVDStream.join(orderDetailKVDStream)
    */
    // 双流join存在问题：
    // 从数据库层面：order_info 和 order_detail表中的数据一定能join成功
    // 从流处理层面：order_info 和 order_detail流中的数据不一定进入同一批次(错位)，join可能失败
    // 数据延迟导致数据没有进入到同一批次，在实时处理中属于正常现象，延时可能导致数据丢失（join失败的数据不会保留）
    // 解决：
    /*
    1、扩大采集周期（延时可能大于周期，仍有较小概率存在数据丢失）
    2、使用窗口（处理过的批次若仍在窗口中，该批次数据还会和其它批次数据进行join，会出现数据重复）
    3、使用fullOuterJoin连接，先保证join成功或失败的数据都保留在结果中
       并使用第三方Redis维护状态，让两个流都多做2步操作：到缓存中找另一个流，把自己写到缓存中
    val orderJoinDStream:DStream[(Long,(Option[OrderInfo],Option[OrderDetail]))] =
        orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)
    orderWidesDStream = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis = RedisUtil.getJedisFromPool
        // 存储Redis维护处理后的结果
        val orderWides:ListBuffer[OverWide] = ListBuffer[OverWide]()
        for ((key,(orderInfoOp,orderDetailOp)) <- orderJoinIter){
          // 1）当前批次：orderInfo有，orderDetail有
          if(orderInfoOp != None){
            // 取出orderInfo
            if(orderDetailOp != None){
              // 取出orderDetail
              // 组装宽表overWide
              // 加入结果
              orderWides.append(overWide)
            }
          // 2）当前批次：orderInfo有，orderDetail没有
          // orderInfo写缓存：类型：string 写入API：setex 读取API：get 是否过期：24小时过期
          // orderInfo读缓存，判断有无orderDetail（依据orderDetail缓存类型set，存在则遍历，组装，加入结果）
          } else {
            // 3）当前批次：orderInfo没有，orderDetail有（orderInfo:orderDetail=>1:多）
            // orderDetail写缓存：类型：set 写入API：sadd 读取API：smembers 是否过期：24小时过期
            // orderDetail读缓存，判断有无orderInfo（存在则组装，加入结果）
          }
        }
        jedis.close()
        orderWides
      }
    )
    */
    // 6、写入ES
    // 如日活宽表写入ES
    /*
    dauInfoDStream.foreachRDD({
      // 去重方式1：幂等写入ES
      rdd => {
        rdd.mapPartitions(dauInfoIter => {
          // 结构转化：dauInfo -> List[(docId,Bean)]
          val docs = dauInfoIter.map(dauInfo=>(dauInfo.uId,dauInfo)).toList
          // 写入ES（ES工具类：cc-Elasticsearch: com.cc.day0413.ESUtil）
          // 定义索引（当天日期直接拼接在后面）
          val index = "dau_info_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          ESUtil.bulk(index,docs)
        })
      }
      // 去重方式2：写入其它OLAP如HBase: Map<String,Object> => Map<唯一标识，Bean>
      rdd => {
        rdd.mapPartitions(dauInfoIter => {
          // 结构转化：dauInfo -> List[(docId,Bean)]
          val list = dauInfoIter.map(dauInfo=>(dauInfo.uId,dauInfo)).toList
          // 表名
          val tableName = "dau_info_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          list.foreach(t=>{
            Try(
              HBaseCrudUtil.put(tableName,t._1,"dauInfo",t._1,t._2)
            ).getOrElse(table.close())
            // 创建表连接，关闭表连接见com.SparkStreaming.day0308.Kafka2Spark2HBase类
          })
        })
      }
      // 7、提交offset
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    })
    */

    ssc.start()
    ssc.awaitTermination()

  }

  // 根据指定唯一标识去重（仅能当前批次去重）
  // List[(String,AnyRef)] => List[(唯一标识,Bean)]
  def idempotence(list:List[(String,AnyRef)]): AnyRef ={
    val values = new util.HashMap()
    val map = new mutable.HashMap[String, AnyRef]()
    list.foreach(t=>{
      map.put(t._1,t._2)
    })
    // Map[K,V] => List[(K,V)] => List[V]
    val res:List[AnyRef] = map.toList.map(_._2)
    res
  }

}
