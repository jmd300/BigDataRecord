package com.zoo.concurrency.book.chapter02

/**
 * @Author: JMD
 * @Date: 3/31/2023
 * 课后题第 3, 4, 5题
 */
class SyncVar[T] {
  var obj: Option[T] = None

  def isEmpty: Boolean = {
    obj match {
      case None => true
      case _ => false
    }
  }
  def notEmpty: Boolean = {
    !isEmpty
  }

  def get(): T = this.synchronized {
    obj match {
      case None => throw new Exception("目前无值，不可以get， 请先put")
      case Some(value) =>
        obj = None
        value
    }
  }
  def put(x: T): Unit = {
    obj match {
      case None => obj = Option(x)
      case _ => throw new Exception("目前有值，不可以put，请先get")
    }
  }
}
object SyncVar{
  def main(args: Array[String]): Unit = {
    val lock = new AnyRef()
    @volatile var isOver = false

    val syncVar = new SyncVar[Int]()
    val producer = new Thread(() => {
      for(num <- 0 until 15){
        while(syncVar.notEmpty){
          lock.wait()
        }

        if(syncVar.isEmpty) {
          syncVar.put(num)
          println("producer: ", num)
          lock.notify()
        }
      }
      isOver = true
    })

    val consumer = new Thread(() => {
      while(!isOver && syncVar.notEmpty){
        while (syncVar.isEmpty){
          lock.wait()
        }
        if(syncVar.notEmpty){
          println("consumer: ", syncVar.get())
          lock.notify()
        }
      }
    })

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()
  }
}
