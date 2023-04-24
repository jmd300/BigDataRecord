package com.zoo.concurrency.book.chapter04


import com.zoo.concurrency.book.concurrency.log

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.NonFatal
import scala.concurrent.duration._

/**
 * @Author: JMD
 * @Date: 4/6/2023
 */
object PromisesCustomAsync {
  private def myFuture[T](b: => T): Future[T] = {
    val p = Promise[T]
    ExecutionContext.global.execute(
      new Runnable {
        override def run(): Unit = try{
          p.success(b)
        } catch {
          case NonFatal(e) => p.failure(e)
        }
      })
    p.future
  }
  def main(args: Array[String]): Unit = {
    val f = myFuture("Hello Promise Future")
    f.foreach(text => log(text))
    val str = Await.result(f, 1.microseconds)
    log(str)
  }
}
