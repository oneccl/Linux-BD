package com.cc.day0413

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.engine.Engine.DeleteResult

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/13
 * Time: 19:08
 * Description:
 */
object ESUtil {

  private val host: String = "bd91"
  private val port: Int = 9200
  private var esClient:RestHighLevelClient = _

  // 创建es客户端对象
  def getEsClient: RestHighLevelClient ={
    if (esClient == null){
      val builder = RestClient.builder(new HttpHost(host, port))
      esClient = new RestHighLevelClient(builder)
    }
    esClient
  }

  // 关闭es客户端连接
  def close(): Unit ={
    if (esClient != null)
    esClient.close()
  }

  // CRUD操作

  // 单条写 增-幂等写: 指定doc id
  def put(index:String, obj:AnyRef, docId:String): Unit ={
    val indexRequest = new IndexRequest()
    // 指定索引
    indexRequest.index(index)
    // 指定doc
    val jsonObj:String = JSON.toJSONString(obj, new SerializeConfig(true))
    indexRequest.source(jsonObj,XContentType.JSON)
    // 指定docId
    indexRequest.id(docId)
    esClient.index(indexRequest,RequestOptions.DEFAULT)

  }

  // 单条写 增-非幂等写: 不指定doc id
  def post(index:String, obj:AnyRef): Unit ={
    val indexRequest = new IndexRequest()
    // 指定索引
    indexRequest.index(index)
    // 指定doc
    val jsonObj:String = JSON.toJSONString(obj, new SerializeConfig(true))
    indexRequest.source(jsonObj,XContentType.JSON)
    esClient.index(indexRequest,RequestOptions.DEFAULT)
  }

  // 批量写 增-幂等写: 指定doc id
//  def bulk(list: List[AnyRef]): Unit ={
//    val bulkRequest = new BulkRequest()
//    for (obj <- list) {
//      val indexRequest = new IndexRequest("index")
//      val jsonObj:String = JSON.toJSONString(obj, new SerializeConfig(true))
//      indexRequest.source(jsonObj,XContentType.JSON)
//      // 幂等写：指定doc id
//      indexRequest.id(obj.id)
//      // 将IndexRequest封装的对象添加到bulk中
//      bulkRequest.add(indexRequest)
//    }
//    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
//  }

  // 批量-幂等写
  // List[(String,AnyRef)] -> List[(docId,Bean)]
  def bulk(index:String,docs: List[(String,AnyRef)]): Unit ={
    val bulkRequest = new BulkRequest(index)
    for (doc <- docs) {
      val indexRequest = new IndexRequest()
      val jsonObj:String = JSON.toJSONString(doc._2, new SerializeConfig(true))
      indexRequest.source(jsonObj,XContentType.JSON)
      // 幂等写：指定doc id
      indexRequest.id(doc._1)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest,RequestOptions.DEFAULT)
  }

  // 批量写 增-非幂等写: 不指定doc id

  // 修改 单条修改
  def update(index:String,docId:String,field:String,value:String): Unit ={
    // 指定索引和id
    val updateRequest = new UpdateRequest(index, docId)
    // 指定要修改的字段和修改后的值
    updateRequest.doc(field,value)
    esClient.update(updateRequest,RequestOptions.DEFAULT)
  }

  // 删除
  def del(index:String,docId:String): Unit ={
    val deleteRequest = new DeleteRequest(index,docId)
    esClient.delete(deleteRequest,RequestOptions.DEFAULT)
  }

  // 查询
  // 单条查询
  def getByDocId(index:String,docId:String): String ={
    val getRequest = new GetRequest(index,docId)
    val response = esClient.get(getRequest, RequestOptions.DEFAULT)
    // 返回结果
    response.getSourceAsString
  }

  // 条件查询

  // 聚合查询


}
