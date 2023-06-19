package com.zoo.flink.scala.operator

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object TransFlatmapDemo extends FlinkEnv{
  class MyFlatMap extends FlatMapFunction[Event, String] {
    @throws[Exception]
    override def flatMap(value: Event, out: Collector[String]): Unit = {
      if (value.user == "Mary") out.collect(value.user)
      else if (value.user == "Bob") {
        out.collect(value.user)
        out.collect(value.url)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 自定义实现类// 自定义实现类
    arrayStream.flatMap(new TransFlatmapDemo.MyFlatMap).print

    // lambda// lambda
    arrayStream.flatMap((value: Event, out: Collector[String]) => {
      value.user match {
        case "Mary" => out.collect(value.user)
        case _ =>
          out.collect(value.user)
          out.collect(value.url)
      }
    }).print

    env.execute
  }
}
