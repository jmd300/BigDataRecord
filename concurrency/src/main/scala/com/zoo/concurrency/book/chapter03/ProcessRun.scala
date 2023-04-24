package com.zoo.concurrency.book.chapter03

import scala.sys.process._
/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
object ProcessRun {
  def main(args: Array[String]): Unit = {
    val command = "cmd /c dir"
    val ret1 = command.!
    println(ret1)
    val ret2 = command.!!

    println(ret2)
  }

}
