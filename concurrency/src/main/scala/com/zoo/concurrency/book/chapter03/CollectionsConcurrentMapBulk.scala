package com.zoo.concurrency.book.chapter03

import com.zoo.concurrency.chapter03.execute

import java.util.concurrent.TimeUnit
import scala.collection.concurrent

/**
 * @Author: JMD
 * @Date: 4/3/2023
 * 没能观察到 scala并发编程中说到的问题
 */
object CollectionsConcurrentMapBulk extends App{
  val names = new concurrent.TrieMap[String, Int]

  execute{for (n <- 0 until 100) {
    names(s"John $n" ) = n
    TimeUnit.MILLISECONDS.sleep(1)
  }}
  TimeUnit.MILLISECONDS.sleep(10)
  execute{for (n <- names) println(s"1-name: $n" )}
  TimeUnit.MILLISECONDS.sleep(10)
  execute{for (n <- names) println(s"2--------------------name: $n" )}
  TimeUnit.MILLISECONDS.sleep(100)



}
