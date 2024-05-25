package com.day0227;

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/17
 * Time: 19:44
 * Description:
 */
public class DemoHBase {

    // HBase读写流程

    // 写数据流程
    /*
    1、客户端请求Zookeeper，Zookeeper返回hbase:meta表所在的RegionServer
    2、客户端访问RegionServer，读取meta表并缓存到metaCache(存储表连接Table)中
    第一次获取Table/创建表会访问RegionServer，之后获取Table不会访问RegionServer
    3、客户端发送put请求(调用put方法)到对应RegionServer
    4、客户端先将数据写入Wal HLog（HDFS），HLog记录着数据的操作日志，当HBase出现
    故障时可以进行日志重放、故障恢复以防数据丢失
    5、客户端将数据顺序写入MemStore缓存，当达到阈值(默认128M)flush到StoreFile(HFile)
    一个表的每个Family对应一个Store，每个Store都会分配一个MemStore(过多Family消耗内存)
    6、多个StoreFile(称为Region文件)会触发Compact合并，当达到阈值会触发Split
    将Region文件一分为二，并实现负载均衡
    */

    // 读数据流程
    /*
    1、客户端请求Zookeeper，Zookeeper返回hbase:meta表所在的RegionServer
    2、客户端访问RegionServer，读取meta表并缓存到metaCache(存储表连接Table)中
    第一次获取Table/创建表会访问RegionServer，之后获取Table不会访问RegionServer
    3、客户端发送get请求(调用get方法)到对应RegionServer
    4、客户端优先访问BlockCache缓存，查找之前是否读取过(对于读取过的数据的元信息会保存在这)
    如果缓存中数据的尾部信息没变(数据未发生变化)，则直接读取缓存数据
    5、若缓存中数据已过期(已发生变化)，则直接扫描StoreFile查询数据
    6、RegionServer响应数据给客户端
    */

}
