package com.zoo.concurrency.book.chapter02

import scala.collection.mutable
import scala.collection.mutable.Queue

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
class SyncQueue[T](capcity: Long) {
  var queue = new mutable.Queue()
  queue

/*  def isEmpty: Boolean = {
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
  }*/
}
