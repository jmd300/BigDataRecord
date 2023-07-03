package com.zoo.flink.scala.operator

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object TupleAggregationDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val stream = env.fromElements(("a", 1), ("a", 3), ("b", 3), ("b", 4))

    stream.keyBy(_._1).sum(1).print
    stream.keyBy(_._1).max(1).print

    stream.keyBy(_._1).min(1).print

    /**
     * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是， min()只计
     * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包
     * 含字段最小值的整条数据。
     *//**
     * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是， min()只计
     * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包
     * 含字段最小值的整条数据。
     */

    stream.keyBy(_._1).minBy(1).print

    stream.keyBy(_._1).maxBy(1).print

    // 如果数据流的类型是 POJO 类，那么就只能通过字段名称来指定，不能通过位置来指定
    arrayStream.keyBy(_.user).max("timestamp").print

    env.execute
  }

}
