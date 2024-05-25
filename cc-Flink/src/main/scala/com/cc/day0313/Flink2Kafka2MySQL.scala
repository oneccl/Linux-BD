package com.cc.day0313

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/13
 * Time: 18:52
 * Description:
 */
object Flink2Kafka2MySQL {

  // Flink-Kafka: 导入依赖flink-connector-kafka_2.12

  def main(args: Array[String]): Unit = {
    // 获取Flink-Streaming实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 消费者连接Kafka配置
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "bd91:9092,bd92:9092,bd93:9092")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("group.id", "group19")
    prop.setProperty("auto.offset.reset", "earliest")
    // Source输入：获取实时处理对象：DataStream
    // old方式1：
    val ds:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]( // 消费对象
      "weblogs", // 主题
      new SimpleStringSchema(), // 值反序列化
      prop // 连接配置
    ).setStartFromGroupOffsets()  // 从上次提交的offset位置继续消费(默认策略：当任务第一次启动，读取不到上次offset，则读取Kafka-offset)
      .setCommitOffsetsOnCheckpoints(true)  // checkPoint时提交offset(默认true)
    )
    //ds.print()  // 拉取打印

    // new方式2：
//    val ks:KafkaSource[String] = KafkaSource.builder()
//      .setTopics("weblogs")
//      .setBootstrapServers("bd91:9092,bd92:9092,bd93:9092")
//      .setDeserializer(SimpleStringSchema[String])
//      .setGroupId("group1")
//      .setProperty("auto.offset.reset", "earliest")
//      .build()
//    val newDs:DataStream[String] = env.fromSource(
//      ks,
//      WatermarkStrategy.noWatermarks(),
//      "kafka_source"
//    )
//    newDs.print()

    // 处理：统计各小时的用户访问量
    val ds1:DataStream[(String,Int)] = ds
      .map(line => {
        val arr = line.split("\\s+")
        val hour = arr(3).split(":")(1)
        val ip = arr.head
        (hour, 1)
      })
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .sum(1)

    // 输出：将结果保存到MySQL
    // Flink-MySQL: 导入依赖flink-connector-jdbc_2.12和MySQL驱动
    // addSink 使用自定义Sink
    ds1.addSink(new JdbcSink)

    // 开启任务：执行实时任务
    env.execute("Kafka-Flink-MySQL")

  }

}

// 自定义JdbcSink，继承RichSinkFunction[IN]，重写3个方法
class JdbcSink extends RichSinkFunction[(String,Int)]{
  var cn:Connection = _
  var create:PreparedStatement = _
  var insert:PreparedStatement = _
  var update:PreparedStatement = _

  // 获取连接
  override def open(parameters: Configuration): Unit = {
    val url = "jdbc:mysql://localhost:3306/day0313_flink?serverTimezone=UTC"
    cn = DriverManager.getConnection(url, "root", "123456")
    create = cn.prepareStatement("create table if not exists hour_uc(hour varchar(10),count int)")
    // execute(): 查询返回ResultSet:True；其它增删改:false
    println("创表: "+ (if(create.execute()) false else true))
    insert = cn.prepareStatement("insert into hour_uc values(?,?)")
    update = cn.prepareStatement("update hour_uc set count=? where hour=?")
  }

  // 执行sql
  override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
    update.setString(1,value._1)
    update.setInt(2,value._2)
    val updateRowNum = update.executeUpdate()
    println("更新: "+updateRowNum)
    // 如果没有执行更新则执行添加(若使用execute()可用update.getUpdateCount获取更新行数)
    if (updateRowNum==0){
      insert.setString(1,value._1)
      insert.setInt(2,value._2)
      val insertRowNum = insert.executeUpdate()
      println("添加: "+insertRowNum)
    }
  }

  // 释放资源
  override def close(): Unit = {
    create.close()
    insert.close()
    update.close()
    cn.close()
  }

}
