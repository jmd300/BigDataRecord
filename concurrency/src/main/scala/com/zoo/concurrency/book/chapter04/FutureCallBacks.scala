package com.zoo.concurrency.book.chapter04


import com.zoo.concurrency.book.concurrency.log

import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
object FutureCallBacks {
  private def getUrlSpec(url: String): Future[List[String]] = Future{

    val f = Source.fromURL(url)
    try f.getLines().toList finally f.close()
  }

  def find(lines: List[String], keyword: String): String = {
    lines.zipWithIndex.collect{
      case (line, n) if line.contains(keyword) => (n, line)
    }.mkString("\n")
  }

  def main(args: Array[String]): Unit = {
    val url = "https://www.gutenberg.org/files/11/11-0.txt"
   val urlSPec = getUrlSpec(url)

    // urlSPec.foreach(lines => log(find(lines, "curious")))
    // urlSPec.failed.foreach(e => log(s"Exception occurred - $e"))

    log("callback registered, continuing with other work")

    urlSPec.onComplete{
      case Success(txt) => log(find(txt, "curious"))
      case Failure(e) => log(s"Exception occurred - $e")
    }

    TimeUnit.MILLISECONDS.sleep(5000)

  }
}
