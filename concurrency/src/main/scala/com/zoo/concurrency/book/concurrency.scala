package com.zoo.concurrency.book

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
package object concurrency {
  def log(msg: String): Unit = {
    println(s"${Thread.currentThread.getName}: $msg")
  }

}
