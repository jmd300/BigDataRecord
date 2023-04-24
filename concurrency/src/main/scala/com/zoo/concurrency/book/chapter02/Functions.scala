package com.zoo.concurrency.book.chapter02

import java.util.concurrent.{FutureTask, TimeUnit}

/**
 * @Author: JMD
 * @Date: 3/31/2023
 * scala 并发编程 第二章
 */
object Functions {
  /**
   * 课后题第一个
   */
  def parallel[A, B](a: => A, b: => B): (A, B) = {
    val futureTaskA = new FutureTask(() => a)

    val futureTaskB = new FutureTask(() => b)

    val taskA = new Thread(futureTaskA)
    val taskB = new Thread(futureTaskB)

    taskA.start()
    taskB.start()

    (futureTaskA.get(), futureTaskB.get())
  }

  /**
   * 课后题第二个
   */
  def periodically(duration: Long)(b: => Unit): Unit = {
    while (true){
      b
      TimeUnit.MICROSECONDS.sleep(duration)
    }
  }



}
