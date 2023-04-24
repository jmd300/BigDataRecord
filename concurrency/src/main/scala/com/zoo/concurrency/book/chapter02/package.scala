package com.zoo.concurrency

/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
package object chapter02 {
  def thread(body: => Unit): Thread = {
    val t = new Thread {
      override def run(): Unit = body
    }
    t.start()
    t
  }
}
