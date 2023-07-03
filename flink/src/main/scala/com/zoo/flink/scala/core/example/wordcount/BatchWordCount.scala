package com.zoo.flink.scala.core.example.wordcount

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * Author: JMD
 * Date: 5/10/2023
 *
 * flink 通过 scala 实现 word count 批处理
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件读取数据 按行读取(存储的元素就是每行的文本)// 从文件读取数据 按行读取(存储的元素就是每行的文本)
    val lineDs: DataSet[String] = env.readTextFile("input/words.txt").setParallelism(1)

    val result = lineDs.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // result.print()

    // 输出结果到文本文件
    val outputPath = "output/scala/word_count"
    result.writeAsText(outputPath)

    // 执行作业，试验证明如果仅仅print，则不需要调用 env.execute()
    env.execute("WordCount Example")
  }
}
