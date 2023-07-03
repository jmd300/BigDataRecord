package com.zoo.lang.scala.test
import scala.collection.mutable.ArrayBuffer

/**
 * Author: JMD
 * Date: 6/27/2023
 */
object ArrayToArrayBuffer {
  def main(args: Array[String]): Unit = {

    val data: Array[Array[AnyRef]] = Array(
      Array("a", "b", "c"),
      Array("d", "e", "f"),
      Array("g", "h", "i")
    )

    val mutData: Array[ArrayBuffer[String]] = data.map(_.map(_.toString).toBuffer.asInstanceOf[ArrayBuffer[String]])

    println(mutData.deep.mkString("\n"))
  }
}
