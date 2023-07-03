package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{DataStreamSource, KeyedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object KeyByDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStreamSource[Event] = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L))

    // 使用 Lambda 表达式，这里不写类型居然推断不出来
    val keyedStream: KeyedStream[Event, String] = stream.keyBy((e: Event) => e.user)

    /**
     * 使用匿名类实现 KeySelector// 使用匿名类实现 KeySelector
     * 需要注意的是， keyBy 得到的结果将不再是 DataStream，而是会将 DataStream 转换为 KeyedStream。
     * KeyedStream 可以认为是“分区流”或者“键控流”，它是对 DataStream 按照key 的一个逻辑分区，所以泛型有两个类型：
     * 除去当前流中的元素类型外，还需要指定 key 的类型。
     */
    val keyedStream1: KeyedStream[Event, String] = stream.keyBy(new KeySelector[Event, String]() {
      @throws[Exception]
      override def getKey(e: Event): String = {
        e.user
      }
    })
  }
}
