package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import com.zoo.flink.java.source.ClickSource
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object ReduceDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    env.addSource(new ClickSource()).map((e: Event) => (e.user, 1L))
      .keyBy(_._1)
      // 分布式计算多个key的count
      .reduce(new ReduceFunction[(String, Long)] {
        override def reduce(t1: (String, Long), t2: (String, Long)): (String, Long) = {
          // 每到一条数据，用户 pv 的统计值加 1
          (t1._1, t1._2 + t2._2)
      }
    }).keyBy(_ => true)
      // 汇总得出count最大的数据
      .reduce((value1, value2) => {
        if (value1._2 > value2._2) value1
        else value2
      })
      .print

    env.execute
  }
}
