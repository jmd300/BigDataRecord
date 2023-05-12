package com.zoo.flink.java.operator;

import com.zoo.flink.java.FlinkEnv;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import com.zoo.flink.java.pojo.Event;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class TransKeyByDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 使用 Lambda 表达式
        KeyedStream<Event, String> keyedStream = arrayStream.keyBy(e -> e.user);

        // 使用匿名类实现 KeySelector
        KeyedStream<Event, String> keyedStream1 = arrayStream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event e) throws Exception {
                return e.user;
            }
        });

        env.execute();
    }
}
