package com.zoo.flink.java.operator;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class FlatmapDemo extends FlinkEnv {
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 自定义实现类
        arrayStream.flatMap(new MyFlatMap()).print();

        // lambda
        arrayStream.flatMap((value, out) -> {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }).print();

        env.execute();
    }
}
