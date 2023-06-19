package com.zoo.flink.java.sink;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class SinkToRedisDemo extends FlinkEnv {
    public static class RedisSinkBuilder implements RedisMapper<Event>  {

        @Override
        public String getKeyFromData(Event e) {
            return e.user;
        }
        @Override
        public String getValueFromData(Event e) {
            return e.url;
        }
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks-java");
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建一个到 redis 连接的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        env.addSource(new ClickSource()).addSink(new RedisSink<Event>(conf, new RedisSinkBuilder()));

        env.execute();
    }
}
