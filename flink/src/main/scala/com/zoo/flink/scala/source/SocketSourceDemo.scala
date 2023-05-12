package com.zoo.flink.scala.source

import com.zoo.flink.scala.FlinkEnv
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 5/11/2023
 */
object SocketSourceDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val inputDataStream = env.socketTextStream("localhost", 7777)

    //对数据进行转换处理
    val resultDataStream = inputDataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1))
      .keyBy(_._1)
      .sum(1)

    //打印输出//打印输出
    resultDataStream.print
  }
}
