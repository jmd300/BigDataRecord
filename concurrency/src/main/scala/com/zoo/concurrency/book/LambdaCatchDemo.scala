package com.zoo.concurrency.book

import java.util.concurrent.TimeUnit

/**
 * @Author: JMD
 * @Date: 3/31/2023
 */

object LambdaCatchDemo {
  def main(args: Array[String]): Unit = {
    var inc: () => Unit = null
    val t = new Thread {
      if (inc != null) inc()
    }
    var number = 1
    inc = () => {
      number += 1
    }
    t.start()
    println(number)
    TimeUnit.SECONDS.sleep(1)
    // 证明了scala lambda表达式中的局部变量是不可变的

    println(number)
    number += 1
    println(number)
  }
}
