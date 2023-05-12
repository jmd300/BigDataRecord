package com.zoo.flink.scala.operator

import com.zoo.flink.scala.FlinkEnv
import com.zoo.flink.java.pojo.Event
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Author: JMD
 * Date: 5/11/2023
 */

object TransMapDemo extends FlinkEnv{
  class UserExtractor extends MapFunction[Event, String] {
    @throws[Exception]
    override def map(e: Event): String = e match {
      case null => null
      case _ => e.user
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      new Event("Mary", "./home", 1000L),
      new Event("Bob", "./cart", 2000L))

    // 1. 传入匿名内部类
    stream.map(new MapFunction[Event, String]{
      override def map(e: Event): String = e match {
        case null => null
        case _ => e.user
      }
    })

    // 2. 传入 MapFunction 的实现类
    stream.map(new UserExtractor())

    // 3. 传入函数
    stream.map((e: Event) => e match {
      case null => null
      case _ => e.user
    })

    env.execute()
  }
}
