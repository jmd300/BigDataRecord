package com.zoo.flink.scala.operator

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.createTypeInformation

/**
 * Author: JMD
 * Date: 6/26/2023
 */
object CustomPartitionDemo extends FlinkEnv{
  private class CustomPartitioner extends Partitioner[Int]{
    override def partition(k: Int, i: Int): Int = k % 2
  }

  def main(args: Array[String]): Unit = {
    // 将自然数按照奇偶分区
    env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
      .partitionCustom(new CustomPartitioner, (e: Int) => e)
      .print().setParallelism(2)
    env.execute();
  }
}
