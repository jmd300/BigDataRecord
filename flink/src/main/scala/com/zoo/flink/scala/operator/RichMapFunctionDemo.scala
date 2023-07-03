package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/12/2023
 *
 * 富函数类提供了 getRuntimeContext()方法（我们在本节的第一个例子中使用了一下），
 * 可以获取到运行时上下文的一些信息，例如程序执行的并行度，任务名称，以及状态（state）。
 * 这使得我们可以大大扩展程序的功能，特别是对于状态的操作，使得 Flink 中的算子具备了处理复杂业务的能力。
 */
object RichMapFunctionDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    arrayStream.map(new RichMapFunction[Event, Long]() {
      @throws[Exception]
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        System.out.println(" 索引为 " + getRuntimeContext.getIndexOfThisSubtask + " 的任务开始")
      }

      @throws[Exception]
      override def map(value: Event): Long = {
        value.timestamp
      }

      @throws[Exception]
      override def close(): Unit = {
        super.close()
        System.out.println(" 索引为 " + getRuntimeContext.getIndexOfThisSubtask + " 的任务结束")
      }
    }).print

    env.execute
  }
}
