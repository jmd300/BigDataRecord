package com.zoo.lang.scala.test
import java.io.File
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

    val dir = "Y:\\学习\\153-Flink核心技术与实战"

    val file = new File(dir)

    val files = file.listFiles()
    files.foreach(e => println(e.getName))

    files.foreach(f => {
      if(f.getName.endsWith("(1).mp4")){
        f.delete()
      }
    })



  }
}
