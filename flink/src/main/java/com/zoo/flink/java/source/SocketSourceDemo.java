package com.zoo.flink.java.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 从socket中读取数据生成流
         */
        DataStream<String> stream = env.socketTextStream("hadoop102", 7777);

        stream.print();
        env.execute();
    }
}
