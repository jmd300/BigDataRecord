package com.zoo.flink.scala.source

import com.zoo.flink.java.util.Event
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random
import java.util.Calendar


/**
 * Author: JMD
 * Date: 6/26/2023
 */
class ClickSource(num: Int = 0) extends SourceFunction[Event]{
  // 声明一个布尔变量，作为控制数据生成的标识位// 声明一个布尔变量，作为控制数据生成的标识位
  private var running = true

  override def run(sourceContext: SourceFunction.SourceContext[Event]): Unit = {
    val random = new Random(); // 在指定的数据集中随机选取数据

    val users = Array("Mary", "Alice", "Bob", "Cary")
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2")

    var count = 0
    while (running && num > count) {
      sourceContext.collect(new Event(users(random.nextInt(users.length)), urls(random.nextInt(urls.length)), Calendar.getInstance.getTimeInMillis))

      // 隔 1 秒生成一个点击事件，方便观测
      Thread.sleep(500)
      count += 1
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
