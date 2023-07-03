package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.FilterFunction

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object FilterDemo extends FlinkEnv{
  class UserFilter extends FilterFunction[Event] {
    @throws[Exception]
    override def filter(e: Event): Boolean = e.user == "Mary"
  }

  def main(args: Array[String]): Unit = {
    // 1. 传入匿名类实现 FilterFunction
    arrayStream.filter(new FilterFunction[Event]() {
      @throws[Exception]
      override def filter(e: Event): Boolean = {
        e.user == "Mary"
      }
    })

    // 2. 传入Function实现类
    arrayStream.filter(new UserFilter)

    // 3. 传入lambda函数
    arrayStream.filter(_.user == "Mary")
  }
}
