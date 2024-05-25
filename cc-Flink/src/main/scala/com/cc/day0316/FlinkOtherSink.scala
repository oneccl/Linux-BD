package com.cc.day0316

import com.day0228.HBaseCrudUtil
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.hadoop.hbase.client.Connection
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/10
 * Time: 8:52
 * Description:
 */
object FlinkOtherSink {

  // Sink-ES、Sink-HBase

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // pojo类型
    val ds:DataStream[Event] = env.fromElements(
      Event("100", "Tom", 18),
      Event("101", "Jack", 17)
    )
    // 1、ES-Sink
    // 连接到的ES集群
    val hosts = List(new HttpHost("bd91", 9200, "http"))
    // Sink-ES 写入ES
    ds.addSink(new ElasticsearchSink[Event](hosts,new EsSink()))

    // 2、HBase-Sink: Flink没有提供HBase连接器，需要自定义Sink（不能保证一致性）
    ds.addSink(new HBaseSink)

    env.execute("Sink-Other")
  }

}

case class Event(var id:String,var name:String,var age:Int)

// ES-Sink
class EsSink extends ElasticsearchSinkFunction[Event]{
  override def process(element: Event, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    val data = new mutable.HashMap[String, String]()
    data.put(element.id,element.name)
    val request = Requests.indexRequest()
      .index("click")    // 索引
      .opType("type")  // 类型
      .source(data)
    indexer.add(request)
  }
}

// HBase-Sink
class HBaseSink extends RichSinkFunction[Event]{
  var cn:Connection = _
  // 获取连接
  override def open(parameters: Configuration): Unit = {
    cn = HBaseCrudUtil.cn
  }
  // 添加数据
  override def invoke(value: Event, context: SinkFunction.Context): Unit = {
    val table = HBaseCrudUtil.getTable("t_name")
    HBaseCrudUtil.put("t_name","rowKey","f1","col_name",value.name)
    table.close()
  }
  // 释放资源
  override def close(): Unit = {
    cn.close()
  }
}
