package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{DataStreamSource, KeyedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object TransKeyByDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStreamSource[Event] = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L))

    // 使用 Lambda 表达式，这里不写类型居然推断不出来
    val keyedStream: KeyedStream[Event, String] = stream.keyBy((e: Event) => e.user)

    // 使用匿名类实现 KeySelector// 使用匿名类实现 KeySelector
    val keyedStream1: KeyedStream[Event, String] = stream.keyBy(new KeySelector[Event, String]() {
      @throws[Exception]
      override def getKey(e: Event): String = {
        e.user
      }
    })
  }
}
