package com.zoo.concurrency.book.chapter02

import com.zoo.concurrency.book.concurrency.log
import com.zoo.concurrency.book.learningConcurrency.thread

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */

object SynchronizedNoDeadlock{
  import SynchronizedProtectedUid._
  class Account(val name: String, var money: Int) {
    val uid: Long = getUniqueId()
  }

  def send(a1: Account, a2: Account, n: Int): Unit = {
    def adjust(): Unit = {
      a1.money -= n
      a2.money += n
    }

    /**
     * 根据 uid的大小 确定/调整 锁获取的先后顺序
     */
    if (a1.uid < a2.uid)
      a1.synchronized { a2.synchronized { adjust() } }
    else
      a2.synchronized { a1.synchronized { adjust() } }
  }

  def main(args: Array[String]): Unit = {
    val a = new Account("Jill", 1000)
    val b = new Account("Jack", 2000)
    val t1 = thread {
      for (_ <- 0 until 100) send(a, b, 1)
    }
    val t2 = thread {
      for (_ <- 0 until 100) send(b, a, 1)
    }
    t1.join()
    t2.join()
    log(s"a = ${a.money}, b = ${b.money}")
  }
}