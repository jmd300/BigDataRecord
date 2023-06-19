package com.zoo.flink.java.util;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class FlinkEnv {
    protected static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    protected static DataStreamSource<Event> arrayStream = env.fromElements(
            new Event("Mary", "./home", 1000L),
            new Event("Bob", "./cart", 2000L),
            new Event("Alice", "./prod?id=100", 3000L),
            new Event("Alice", "./prod?id=200", 3500L),
            new Event("Bob", "./prod?id=2", 2500L),
            new Event("Alice", "./prod?id=300", 3600L),
            new Event("Bob", "./home", 3000L),
            new Event("Bob", "./prod?id=1", 2300L),
            new Event("Bob", "./prod?id=3", 3300L));
    static {
        env.setParallelism(1);
    }
}
