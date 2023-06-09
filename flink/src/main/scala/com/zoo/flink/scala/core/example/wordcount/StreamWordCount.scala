package com.zoo.flink.scala.core.example.wordcount

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.streaming.api.scala._

/**
 * Author: JMD
 * Date: 5/10/2023
 */
object StreamWordCount extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val inputDataStream: DataStream[String] = env.readTextFile("input/words.txt")

    // 根据空格对数据进行切分，统计每个单词出现的次数
    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    resultDataStream.print()

    env.execute()
  }
}
