package com.zoo.flink.scala.sink

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SinkToKafkaDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.put("bootstrap.servers", "hadoop102:9092")
    val stream = env.readTextFile("input/words.txt")

    stream.addSink(new FlinkKafkaProducer[String]("flink-demo", new SimpleStringSchema, properties))

    env.execute
  }
}
