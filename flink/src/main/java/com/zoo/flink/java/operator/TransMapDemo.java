package com.zoo.flink.java.operator;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class TransMapDemo extends FlinkEnv {
    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event e) throws Exception {
            return e == null ? null : e.user;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 传入匿名类，实现 MapFunction
        arrayStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event e) throws Exception {
                return e == null ? null : e.user;
            }
        });

        // 2. 传入 MapFunction 的实现类
        arrayStream.map(new UserExtractor()).print();

        // 3. lambda 函数
        arrayStream.map(event -> {
            return event == null ? null : event.user;
        }).returns(String.class);

        env.execute();
    }
}
