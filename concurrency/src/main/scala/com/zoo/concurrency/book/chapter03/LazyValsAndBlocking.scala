package com.zoo.concurrency.book.chapter03

import java.util.concurrent.TimeUnit

/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
object LazyValsAndBlocking extends App{
  lazy val x: Int = {
    println("111")
    var data = 0
    val t = new Thread(() => {
      println(s"thread Initializing start")
      println(s"thread Initializing $x")
      println(s"thread Initializing over")
      data = 1
    })
    t.start()
    println("222")
    t.join()
    data
  }
  TimeUnit.SECONDS.sleep(1)

  x
  println(s"main Initializing $x")
}
