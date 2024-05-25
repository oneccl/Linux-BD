package com.SparkStreaming.day0308

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Created with IntelliJ IDEA.
 * Author: CC
 * E-mail: 203717588@qq.com
 * Date: 2023/4/11
 * Time: 15:43
 * Description:
 */

object RedisUtil {

  private var jedisPool:JedisPool=_

  // 连接Redis
  def getJedisFromPool: Jedis ={
    if (jedisPool==null){
      // 创建Jedis连接池对象
      val config = new JedisPoolConfig
      config.setMaxTotal(100)      // 最大连接数
      config.setMaxIdle(20)        // 最大空闲数
      config.setMinIdle(20)        // 最小空闲数
      config.setBlockWhenExhausted(true)  // 忙碌时是否等待
      config.setMaxWaitMillis(5000)       // 最大等待时间5s
      config.setTestOnBorrow(true)        // 每次获取连接测试
      val host = "bd91"
      val port = "6379"
      jedisPool = new JedisPool(config,host,port.toInt)
    }
    jedisPool.getResource
  }

}
