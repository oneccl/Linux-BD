package com.SparkStreaming.day0308

import com.day0228.HBaseCrudUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.util.Try
import java.util

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/9
 * Time: 17:13
 * Description:
 */
object Kafka2Spark2HBase {

  // Kafka-Spark-Kafka/HBase
  // 使用Redis维护Kafka-offset
  // 写入Kafka/HBase优化
  /*
  1）原来: ds.foreach(rdd=>{单条数据写入HBase}): 每条数据写入HBase都需要创建1次连接
  获取1次Table实例，资源开销较大，通常数秒才能完成，效率较低，性能较差
  2）优化: ds.foreach(rdd=>{rdd.foreachPartition{分区数据写入HBase}}): 全局连接
  且每个分区仅获取1次Table实例，资源消耗少，性能大大提升
  */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SinkKafkaHBase").master("local[2]").getOrCreate()

    val sc = spark.sparkContext
    sc.setCheckpointDir("C:\\Users\\cc\\Desktop\\StreamingHBase")

    // 获取Streaming实时处理对象
    val ssc = new StreamingContext(sc,Seconds(2))

    // Kafka拉取数据配置
    val confMap = Map(
      ("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"),
      ("bootstrap.servers","bd91:9092,bd92:9092,bd93:9092"),
      ("group.id","g10"),
      ("auto.offset.reset","earliest"),
      ("enable.auto.commit","false")
    )
    // 1、Source：Kafka
    // 获取kafka操作对象
    var ds:DStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // 轮循获取

      /* ----- Redis:如何让SparkStreaming通过指定offset进行消费？ ----- */
      // def Subscribe[K, V](
      //      topics: Collection[String],
      //      kafkaParams: Map[String, Object],
      //      offsets: Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {}

      ConsumerStrategies.Subscribe[String, String]( // 订阅主题，获取数据
        Iterable("topic"),
        confMap
      )
    )

    /* ----- Redis:Redis维护offset优化 ------ */
    // 从Redis读取offsets，指定offsets进行消费
    val redisOffsets = OffsetManager.getOffset("topic", "groupId")
    if (redisOffsets != null && redisOffsets.nonEmpty){
      // 指定offset消费
      ds = KafkaUtil.getKafkaDStream(ssc,Array("topic"),"groupId",redisOffsets)
    } else {
      // 使用Kafka默认offset消费
      ds = KafkaUtil.getKafkaDStream(ssc,Array("topic"),"groupId")
    }

    /* ----- Redis:从Kafka消费到数据，提取offset ----- */
    // 补充：从当前消费到的数据中提取offsets，不对流中的数据做任何处理
    var offsetRanges:Array[OffsetRange] = null
    val ds1 = ds.transform(rdd => {
      // 从RDD中提取offsets（Driver端执行）
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 不对流中的数据做任何处理，返回RDD
      rdd
    })

    // 2、计算
    // ......
    // 3、Sink：HBase
    // 获取HBase连接（工具类中已连接）
    //val cn = HBaseCrudUtil.cn
    // 仅transform、foreachRDD在Driver端执行，其它算子在Executor端执行
    /* 位置A：Driver端执行，每启动程序执行一次 */
    ds1.foreachRDD(rdd=>{
      /* 位置B：Driver端执行，每一个批次执行一次 */

      /* 动态获取表清单 */
      val jedis = RedisUtil.getJedisFromPool
      // 获取事实清单表
      val factTables:util.Set[String] = jedis.smembers("fact:tables")
      // 保存到事实清单表数据到广播变量中
      val factTablesBc = sc.broadcast(factTables)
      // 获取维度清单表
      val dimTables:util.Set[String] = jedis.smembers("dim:tables")
      // 保存到维度清单表数据到广播变量中
      val dimTablesBc = sc.broadcast(dimTables)
      jedis.close()

      // 遍历每条数据/每个分区
      rdd.foreachPartition((consumerRecord:Iterator[ConsumerRecord[String,String]])=>{
        /* 位置C：Executor端执行，foreach每条数据执行一次，foreachPartition每分区执行一次 */
        consumerRecord.foreach(record=>{

          /* 获取广播变量的内容 */
          val factTables = factTablesBc.value
          val dimTables = dimTablesBc.value
          /* 明确操作(表名)，提取数据 */

          val data = record.value()
          if (data!=null && !data.equals("")){
            // 创建命名空间（数据库）
            HBaseCrudUtil.createNamespace("SparkSink")
            // 创建HBase表
            HBaseCrudUtil.createTable("SparkSink:data","logInfo")
            // rowKey
            val rowKey = System.currentTimeMillis().toString
            // 表格对象
            val table = HBaseCrudUtil.getTable("SparkSink:data")
            // 添加数据，若出错关闭连接
            Try(
              HBaseCrudUtil.put("SparkSink:data",rowKey,"logInfo","col",data)
            ).getOrElse(table.close())
            // 分区数据写入HBase后关闭连接
            table.close()

            /* ----- Redis:将结果保存到Kafka-topic ----- */
            KafkaUtil.send("topic","data")

          }
        })

        /* ----- Redis:刷新Kafka缓存 ----- */
        KafkaUtil.flush()

      })
      // offset手动提交方式1：
      // 获取offset，offset存储于HasOffsetRanges
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 异步提交offset，更新Kafka offset
      // 转换提交只适合DStream[ConsumerRecord[String,String]]类型的ds
      // 存在问题：数据经过处理(如转换)后ds类型发生变化，无法使用如下语句提交偏移量
      // commitAsync会先将offset提交到队列中，当下次拉取时，才会将队列中的offset进行commit
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsets)

      // offset手动提交方式2：先存储offset，再进行处理，最后提交
      /* ----- Redis:Redis维护offset提交位置: 将数据处理完成后发送到Kafka-Topic ----- */
      OffsetManager.saveOffset("topic","groupId",offsetRanges)
    })

    // 开启任务
    ssc.start()
    ssc.awaitTermination()

    /* ----- Redis:Redis-offset提交发送到Kafka-topic问题及优化 ----- */
    // 问题：Producer -> Kafka缓冲区 -> Topic-Partition
    // 数据可能存在丢失
    // 优化方案：
    // 1）同步提交：acks=all，每条消息写入Leader和Follower后返回ACK（回调对象），性能较低，降低了Kafka吞吐量
    // 2）Producer -> Kafka缓冲区 -> producer.flush（提交offset前刷新缓存） -> Topic-Partition
    // 若在 位置C 且使用foreach方法，效果同同步提交，有几条消息，刷新几次
    // 若在 位置A、B 则Driver端和Executor端的Producer对象不同，无法刷新
    // 优化：位置C 且使用foreachPartition方法，分区内刷新，有几个分区刷新几次，性能较高
    // 3）Kafka事务

    /* 其它问题及优化 */
    /*
    1）MySQL历史数据（全量/维度数据）同步到Redis（维度数据(本身是json)存储到Redis，事实数据(清洗后封装为对象转为json)存储到Kafka）
    方法1：单独开发程序: 查询MySQL保存历史数据到Redis（×）数据来源多，不方便管理
    方法2：单独开发程序: 查询MySQL保存历史数据到Kafka（√）多路复用
    方法3：使用Maxwell: 全量同步历史数据到Kafka（最优）多路复用
    2）Redis频繁开关
    解决：位置C 选择foreachPartition，每个分区执行一次
    位置A、B 在Driver端执行，Jedis要在Executor中执行，需要传输Jedis对象，而Jedis对象不支持序列化传输
    3）如何动态维护表清单(事实表、维度表)
    解决：维护到Redis中，实时任务动态的从Redis中获取表清单，表的增减直接操作Redis，不直接修改代码，达到解耦效果
    类型：set
    key：fact:tables dim:tables
    value：表名的set集合
    写入API：sadd 读取API：smembers
    是否过期：永不过期
    */
    // 获取动态表清单位置：位置B: 每批次数据获取一次
    // 优化：获取表清单数据在Driver中执行，需要发送给Executor中的每个Task，性能较低
    // 解决：可以使用广播变量保存数据，每个Executor执一份，多个Task共享该一份数据

    // 4）数据的处理顺序性
    /*
    问题：
    a、Kafka单分区可以保证数据的顺序性，多分区不能保证
    b、若到达Spark的数据是有序的，且Spark算子中没有重分区操作，可以保证数据的顺序性，否则不能
    解决：在Maxwell配置文件中配置往Kafka发送数据时要不要做分区操作
    Kafka默认使用hash(key)%分区数对数据进行分区，对同一条数据，可以使用该对象的唯一属性id进行分区
    # 按表中的某列分区
    producer_partition_by = column
    producer_partition_columns = id
    # 若字段不存在，则按表进行分区
    producer_partition_by_fallback = table
    */

  }

}
