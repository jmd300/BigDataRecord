package com.zoo.concurrency.book

/**
 * @Author: JMD
 * @Date: 3/29/2023
 */
package object learningConcurrency {
  def thread(body: => Unit): Thread = {
    val t = new Thread {
      override def run() = body
    }
    t.start()
    t
  }
}
