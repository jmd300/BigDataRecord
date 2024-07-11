package com.zoo.lang.scala.test

object TypeExample {
  def main(args: Array[String]): Unit = {

    val num: Int = 3
    //##
    val s = "scala"
    val hashCode1 = s.##
    val hashCode2 = s.hashCode
    println(hashCode1)
    println(hashCode2)

    // None 是一个实例，不是类，是Option[Nothing]的实例/对象
    println(None)
    println(None.getClass)
    println(None.isInstanceOf[Option[Nothing]])
  }
}
