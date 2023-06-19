package com.zoo.flink.scala.sink

import com.zoo.flink.java.source.ClickSource
import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SinkToRedisDemo extends FlinkEnv{
  class RedisSinkBuilder extends RedisMapper[Event] {
    override def getKeyFromData(e: Event): String = e.user
    override def getValueFromData(e: Event): String = e.url
    override def  getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "clicks-scala")
  }
  def main(args: Array[String]): Unit = {
    // 创建一个连接到 redis 的配置
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build

    env.addSource(new ClickSource).addSink(new RedisSink[Event](conf, new SinkToRedisDemo.RedisSinkBuilder))

    env.execute
  }
}
