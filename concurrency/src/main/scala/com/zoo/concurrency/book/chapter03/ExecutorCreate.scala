package com.zoo.concurrency.book.chapter03

import java.util.concurrent.TimeUnit
import scala.concurrent._
/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
object ExecutorCreate {
  def main(args: Array[String]): Unit = {
    val executor = new forkjoin.ForkJoinPool
    executor.execute(new Runnable {
      override def run(): Unit = println("This task is run asynchronously")
    })
    println(executor.awaitQuiescence(10, TimeUnit.SECONDS))
  }
}
