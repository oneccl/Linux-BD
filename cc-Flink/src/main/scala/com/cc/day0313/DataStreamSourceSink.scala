package com.cc.day0313

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/3/14
 * Time: 10:29
 * Description:
 */
object DataStreamSourceSink {

  // Flink-Streaming实时处理输入输出

  def main(args: Array[String]): Unit = {
    // 获取Flink-Streaming实时处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1、Flink-Streaming-Source 输入

    // 1.1、socket获取
    val ds1 = env.socketTextStream("bd91", 8888)
    // 1.2、Text文件/目录输入（可指定编码）
    val ds2 = env.readTextFile("")
    // 1.3、Flink-Kafka 读取Kafka数据
    // KafkaSource: 见Flink2Kafka2MySQL单例类

    // 1.4、Flink-HDFS 需要导入依赖flink-hadoop-compatibility_2.12和Hadoop客户端依赖
    val ds3 = env.readTextFile("hdfs://bd91:8020/Harry.txt")
    ds3.print()

    // 1.5、addSource 自定义Source（一般用于测试）

    // 2、Flink-Streaming-Sink 输出

    // 2.1、输出到文件
    val fileSink = StreamingFileSink.forRowFormat[String](
      new Path("输出路径"),
      new SimpleStringEncoder[String]("UF-8") // 编码
    ).withRollingPolicy( // 文件生成滚动策略
      DefaultRollingPolicy.builder()
        .withMaxPartSize(2 * 1024 * 1024) // 2M滚动生成新文件
        .withRolloverInterval(TimeUnit.DAYS.toDays(1)) // 1天滚动一次生成新文件
        .build()
    ).build()
    ds3.addSink(fileSink)

    // 2.2、Flink-Kafka 输出到Kafka
    val prop = new Properties()  // 生产者配置
    prop.setProperty("bootstrap.servers", "bd91:9092,bd92:9092,bd93:9092")
    prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // Sink输出
    val dss:DataStreamSink[String] = ds3.addSink(new FlinkKafkaProducer[String](
      "topic",
      new SimpleStringSchema(),
      prop
    ))

    // 2.2、Flink-MySQL 输出到数据库
    // 方式1: 见Flink2Kafka2MySQL单例类
    // 方式2: 需要将元素转换为Event(name,age)对象添加（自定义Event类）
//    ds1.addSink(
//      // public static <T> SinkFunction<T> sink(
//      //            String sql,
//      //            JdbcStatementBuilder<T> statementBuilder,
//      //            JdbcConnectionOptions connectionOptions) {}
//      JdbcSink.sink(
//        "insert into t_name(f1,f2) values(?,?)",
//        (statement,v)=>{
//          statement.setString(1,v.f1)
//          statement.setString(2,v.f2)
//        },
//        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//          .withUrl("jdbc:mysql://localhost:3306/database")
//          .withDriverName("com.mysql.cj.jdbc.Driver")
//          .withUsername("root")
//          .withPassword("123456")
//          .build()
//      )
//    )

    // 开启任务：执行实时任务
    env.execute("Flink-Streaming-Sink")

  }

}
