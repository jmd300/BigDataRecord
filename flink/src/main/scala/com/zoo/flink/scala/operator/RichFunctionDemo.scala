package com.zoo.flink.scala.operator

import com.zoo.flink.java.pojo.Event
import com.zoo.flink.scala.FlinkEnv
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object RichFunctionDemo extends FlinkEnv{
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
