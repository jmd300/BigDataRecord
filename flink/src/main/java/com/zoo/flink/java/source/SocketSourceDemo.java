package com.zoo.flink.java.source;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class SocketSourceDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 从socket中读取数据生成流
        DataStream<String> stream = env.socketTextStream("hadoop102", 7777);

        stream.print();
        env.execute();
    }
}
