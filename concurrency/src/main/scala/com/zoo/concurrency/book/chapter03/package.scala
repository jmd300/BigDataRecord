package com.zoo.concurrency

import scala.concurrent.ExecutionContext

/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
package object chapter03 {
  def execute(body: => Unit): Unit = ExecutionContext.global.execute(new Runnable {
    def run(): Unit = body
  })
}

