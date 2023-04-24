package com.zoo.concurrency.book.chapter04

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
import scala.util.{Try, Success, Failure}
object FuturesTry {
  def main(args: Array[String]): Unit = {
    val threadName = Try(Thread.currentThread().getName)
    val someText = Try("Try Object are synchronous")

    val message: Try[String] = for {
      tn <- threadName
      st <- someText
    }yield s"Message $st was created on t = $tn"

    println(message)
  }
}
