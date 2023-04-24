package com.zoo.concurrency.book.chapter03

import java.util.concurrent.TimeUnit
import scala.collection.concurrent

/**
 * @Author: JMD
 * @Date: 4/3/2023
 */
object CollectionsTrieMapBulk extends App{
  val names = new concurrent.TrieMap[String, Int]
  names("Janice") = 0
  names("Jackie") = 0
  names("Jill") = 0
  val t1 = new Thread(() => {
      for(n <- 10 until 100){
        names(s"John $n") = n
        TimeUnit.MILLISECONDS.sleep(1)
      }
    })
  t1.start()
  // TimeUnit.MILLISECONDS.sleep(100)
  TimeUnit.MILLISECONDS.sleep(2)
  val t2 = new Thread(() => {
      println("snapshot")
      for(n <- names.keys.toSeq.sorted){
        println(s"name: $n")
      }
    })
  t2.start()

  t1.join()
  t2.join()
  println(names.keys)
}







