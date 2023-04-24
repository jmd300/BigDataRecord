package com.zoo.concurrency.book

import com.zoo.concurrency.book.learningConcurrency.thread

/**
 * @Author: JMD
 * @Date: 3/29/2023
 */

object SyncFuncDemo {
  private var uidCount = 0

  private def getUniqueId(): Int = {
    this.synchronized {
      val freshUid = uidCount + 1
      uidCount = freshUid
      freshUid
    }
  }

  def main(args: Array[String]): Unit = {
    def printUniqueIds(n: Int): Unit = {
      val uidArr = for (i <- 0 until n) yield getUniqueId()
      println(uidArr.mkString(", "))
    }

    val t = thread {
      printUniqueIds(5)
    }
    printUniqueIds(5)
    t.join()
  }
}
