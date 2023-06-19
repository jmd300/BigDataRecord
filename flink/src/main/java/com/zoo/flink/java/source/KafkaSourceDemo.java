package com.zoo.flink.java.source;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 * 1. 创建topic
 *   bin/kafka-topics.sh --create --topic flink-demo --bootstrap-server hadoop102:9092
 * 2. 命令行作为生产者
 *   bin/kafka-console-producer.sh --topic flink-demo --bootstrap-server hadoop102:9092
 * 3. 本程序作为消费者
 */
public class KafkaSourceDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "flink-group");

        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("flink-demo", new SimpleStringSchema(), properties));

        // 得到了虚拟机中使用kafka命令行作为生产者生产的数据
        stream.print("Kafka");

        env.execute();
    }
}
