package com.zoo.flink.scala.wordcount

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * Author: JMD
 * Date: 5/10/2023
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件读取数据 按行读取(存储的元素就是每行的文本)// 从文件读取数据 按行读取(存储的元素就是每行的文本)
    val lineDs: DataSet[String] = env.readTextFile(args(0)).setParallelism(1)

    lineDs.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).sum(1).print()

    env.execute()
  }
}
