package com.zoo.concurrency.book.chapter04

import com.zoo.concurrency.book.concurrency.log

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import java.util.concurrent.TimeUnit

/**
 * @Author: JMD
 * @Date: 4/6/2023
 */
object PromisesCreate {
  def main(args: Array[String]): Unit = {
    val p = Promise[String]
    val q = Promise[String]
    // p的结束值
    p success "assigned 1"

    // 对p的结果做处理
    p.future.foreach (x => log(s"p succeeded with $x"))


    // q的结束值
    q failure new Exception("not kept")

    //对q的结果做处理
    q.future.failed.foreach(t => log(s"q failed with $t"))

    TimeUnit.MILLISECONDS.sleep(1000)

  }
}
