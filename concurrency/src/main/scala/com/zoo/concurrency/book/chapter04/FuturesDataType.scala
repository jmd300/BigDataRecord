package com.zoo.concurrency.book.chapter04


import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, Future, blocking}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
object FuturesDataType {

  def main(args: Array[String]): Unit = {
    val buildFile: Future[String] = Future{
      val f = Source.fromFile("Y:\\github\\BigDataRecord\\concurrency\\pom.xml")
      val ret = try f.getLines().mkString("\n") finally f.close()
      println(ret)
      ret
    }

    // buildFile.foreach(s => log(s"start reading the maven pom file asynchronously: $s"))
    while (!buildFile.isCompleted){
      TimeUnit.MILLISECONDS.sleep(100)
    }
    buildFile.failed foreach{
      case e: Exception => ""
    }
  }
}
