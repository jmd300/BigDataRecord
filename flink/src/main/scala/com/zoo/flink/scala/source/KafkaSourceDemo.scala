package com.zoo.flink.scala.source

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object KafkaSourceDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "flink-group")

    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    properties.setProperty("auto.offset.reset", "latest")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("flink-demo", new SimpleStringSchema, properties))

    // 得到了虚拟机中使用kafka命令行作为生产者生产的数据// 得到了虚拟机中使用kafka命令行作为生产者生产的数据
    stream.print("Kafka")

    env.execute
  }
}
