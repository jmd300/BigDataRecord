package com.zoo.flink.java.sink;

import com.zoo.flink.java.FlinkEnv;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class SinkToKafkaDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        DataStreamSource<String> stream = env.readTextFile("input/words.txt");
        stream.addSink(new FlinkKafkaProducer<String>(
                        "flink-demo",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();
    }
}
