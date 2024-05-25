package com.SparkStreaming.day0308

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/18
 * Time: 9:15
 * Description:
 */

// A、日志数据分流：OdsBaseLog
// Flume->Kafka统一主题(ODS)->Streaming分流->Kafka对应Topic(DWD)
/*
1、准备实时环境
2、从Redis读取偏移量offset
3、从Kafka中消费数据（指定主题：统一主题：ods_base_log，主题定义在Maxwell配置文件中，指定消费组）
4、提取偏移量结束点
5、处理数据
5.1、转换数据结构(JsonObject)：专用结构：自定义Bean；通用结构：Map、JsonObject
5.2、分流：OdsBaseLog：日志数据写出到Kafka对应Topic
         OdsBaseDB：事实数据->Kafka主题；维度数据->Redis
6、刷新Kafka缓冲区
7、提交offset
*/
// B、业务数据分流：OdsBaseDB
// Maxwell->Kafka统一主题(ODS)->Streaming分流->
// 事实数据：Kafka对应Topic(DWD):订单Topic、明细Topic、支付Topic、收藏Topic、...
// 维度数据：Redis:用户、商品、地区、种类、...

object OdsBaseLog {

  def main(args: Array[String]): Unit = {
    // 1、准备实时环境
    // 2、从Redis读取偏移量offset
    // 3、从Kafka中消费数据：获取kafkaDStream
    // 4、提取偏移量结束点
    // 5、处理数据
    // 5.1、转换数据结构
    /*
    jsonObjDStream = kafkaDStream.map(consumerRecord=>{
      // 获取ConsumerRecord中的value，value就是日志数据
      val jsonLog:String = consumerRecord.value
      val jsonObj:JSONObject = JSON.parseObject(jsonLog)
      jsonObj
    })
    */
    // 5.2、日志分流：拆成5部分分流到不同Topic(DWD)中
    // log日志数据(2部分混合)：

    // 第一部分：页面访问数据：
    /*
    {
      "common":{
      },
      "page":{
      },
      "displays":[
        {
        },
        ...
      ],
      "actions":[
        {
        },
        ...
      ],
      "error":{
      },
      "ts":时间戳
    }
    */
    /*
    公共字段（共享给其它部分）
    页面数据（部分1）
    曝光数据（部分2）
    事件数据（部分3）
    错误数据（部分5）
    */
    // 第二部分：启动数据：
    /*
    {
      "common":{
      },
      "start":{
      },
      "error":{
      }
    }
    */
    /*
    公共字段（共享给其它部分）
    启动数据（部分4）
    错误数据（部分5）
    */
    // 分流规则：
    /*
    错误数据：不做任何拆分，包含error字段的直接发送到对应Topic（可不处理）
    页面数据：拆分为页面访问、曝光、事件分别发送到对应Topic
    启动数据：不做任何拆分，包含start字段的直接发送到对应Topic
    */

    // 日志分流
    /*
    jsonObjDStream.foreachRDD(rdd=>{
      rdd.foreach(jsonObj=>{
        // 1、提取错误数据
        val errObj = jsonObj.getJSONObject("error")
        if (errObj != null){
          // 发送到Kafka-Topic
        } else {
          // 2、提取公共字段
          val commonObj = jsonObj.getJSONObject("common")
          val uId:String = commonObj.getString("uId")
          val mId:String = commonObj.getString("mId")
          ...
          // 3、页面数据
          val pageObj = jsonObj.getJSONObject("page")
          if (pageObj != null){
            // 4、提取page字段
            val pageId:String = commonObj.getString("page_id")
            val lastPageId:String = commonObj.getString("last_page_id")
            ...
            // 发送到对应主题dwd_page_log（封装Bean->转为Json）

            // 5、提取事件数据（数组类型）：拼接页面数据、公共字段、事件数据（Bean）
            val actionsArrObj = jsonObj.getJSONObject("actions")
            if (actionsArrObj != null && actionsArrObj.size() > 0){
              for(i <- 0 until actionsArrObj.size()){
                val actionObj = actionsArrObj.getJSONObject(i)
                val actionId = actionObj.getString("action_id")
                ...
                // 写出到对应主题dwd_page_action_log（封装Bean->转为Json）
              }
            }

            // 6、提取曝光数据（数组类型）：操作同事件数据
            ...
          }
          // 7、启动数据
          ...
        }

      })
    })
    */

    // 业务数据格式：
    /*
    {
      "database":"数据库名"
      "xid":
      "data":{
        "id":
        "name":
        "login_name":
        "gender":
        "birthday":
        "phone_num":
        "email":
      }
      "old":{
        "修改的字段":"原来的值"
      }
      "type":"insert/update:修改后的结果放在data中，原来的修改属性放在old中/delete/ddl"
      "table":"表名(user_info)"
      "ts":"时间"
    }
    */
    // 业务数据分流
    // 事实表清单（缺啥补啥）
    val factTables:Array[String] = Array("order_info","order_detail")
    // 维度表清单（缺啥补啥）
    val dimTables:Array[String] = Array("user_info","base_province")
    // 优化：见Kafka2Spark2HBase中：动态表清单维护
    /*
    jsonObjDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(jsonIter=>{
        for (jsonObj <- jsonIter){
          // 1、选取操作类型
          val opType:String = jsonObj.getString("type")
          val opValue:String = opType match {
            case "insert" => "I"
            case "update" => "U"
            case "delete" => "D"
            case _ => null
          }
          // 判断操作类型，过滤数据
          if(opValue != null){
            // 提取表名
            val tableName:String = jsonObj.getString("table")
            // 事实数据
            if(factTables.contains(tableName)){
              // 提取数据: String类型
              val data:String = jsonObj.getString("data")
              val dwdTopicName:String = s"DWD_${tableName.toUpperCase}_${opValue}"
              KafkaUtil.send(dwdTopicName,data)
            }
            // 维度数据
            if(dimTables.contains(tableName)){
              // 类型：string: 一条数据存成一个json
              // key：DIM:表名:ID
              // value：每条数据的json格式
              // 过期：不过期
              // 提取数据: jsonObj类型
              val dataObj:JSONObject = jsonObj.getJSONObject("data")
              val id:String = dataObj.getString("id")
              // 拼接K-V，存入Redis
            }
          }
        }
        // 刷新Kafka缓冲区
      })
      // 提交offset
    }
    */

  }

}
