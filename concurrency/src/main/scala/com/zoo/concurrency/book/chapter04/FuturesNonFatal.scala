package com.zoo.concurrency.book.chapter04


import com.zoo.concurrency.book.concurrency.log

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
object FuturesNonFatal{
  def main(args: Array[String]): Unit = {
    // val f = Future {throw new Exception()}
    // val f = Future {throw new InterruptedException()}
    // val f = Future {"hello"}
    val f = Future {8 / 0}
    val g = Future {throw new IllegalArgumentException()}

    var a = 0

    /**
     * k恶意捕获一般异常，不能捕获致命异常
     */
    f.failed.foreach{
      case NonFatal(t) =>
        print("non-fatal")
        log(s"$t is non-fatal")
        a = 5
      case _ => println("致命异常")
    }

    println(s"a is $a")
    println("over")
    TimeUnit.MILLISECONDS.sleep(1000)
  }
}
