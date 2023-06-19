package com.zoo.flink.scala.source

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.createTypeInformation

import java.util.Random
import java.util.concurrent.TimeUnit

/**
 * Author: JMD
 * Date: 5/12/2023
 */

object ParallelSourceExample extends FlinkEnv{
  class CustomSource extends ParallelSourceFunction[(String, Int)] {
    private var running = true
    private val random = new Random


    def cancel(): Unit = running = false

    @throws[Exception]
    override def run(ctx: SourceFunction.SourceContext[(String, Int)]): Unit = {
      while (running) {
        ctx.collect((Thread.currentThread().getName, random.nextInt))
        TimeUnit.MILLISECONDS.sleep(1000)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 可以看到是多个线程并行处理的
    env.addSource(new CustomSource).setParallelism(2).print

    env.execute
  }
}
