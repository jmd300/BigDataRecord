package com.zoo.concurrency.book.chapter02

import com.zoo.concurrency.book.learningConcurrency.thread

/**
 * @Author: JMD
 * @Date: 4/4/2023
 *
 * 多个线程交替获取递增的 uidCount
 */
object SynchronizedProtectedUid{
  var uidCount = 0L

  def getUniqueId(): Long = this.synchronized {
    val freshUid = uidCount + 1
    uidCount = freshUid
    freshUid
  }

  def printUniqueIds(n: Int): Unit = {
    val uids = (0 until n).toArray.map(_ => getUniqueId())
    println(s"Generated uids: ${uids.mkString("Array(", ", ", ")")}")
  }

  def main(args: Array[String]): Unit = {
    val t = thread {printUniqueIds(5)}

    printUniqueIds(5)

    t.join()
  }
}
