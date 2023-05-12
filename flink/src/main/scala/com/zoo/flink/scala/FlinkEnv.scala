package com.zoo.flink.scala

import com.zoo.flink.java.pojo.Event

// 这里注意别引用错误的类
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * Author: JMD
 * Date: 5/11/2023
 */
class FlinkEnv {
  protected var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val arrayStream: DataStream[Event] = env.fromElements(
    new Event("Mary", "./home", 1000L),
    new Event("Bob", "./cart", 2000L),
    new Event("Alice", "./prod?id=100", 3000L),
    new Event("Alice", "./prod?id=200", 3500L),
    new Event("Bob", "./prod?id=2", 2500L),
    new Event("Alice", "./prod?id=300", 3600L),
    new Event("Bob", "./home", 3000L),
    new Event("Bob", "./prod?id=1", 2300L),
    new Event("Bob", "./prod?id=3", 3300L)
  )
}
