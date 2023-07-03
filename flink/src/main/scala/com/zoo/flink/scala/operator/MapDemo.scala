package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/11/2023
 */

object MapDemo extends FlinkEnv{
  class UserExtractor extends MapFunction[Event, String] {
    @throws[Exception]
    override def map(e: Event): String = e match {
      case null => null
      case _ => e.user
    }
  }

  def main(args: Array[String]): Unit = {
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
