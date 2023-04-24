package com.zoo.concurrency.book.chapter04

import com.zoo.concurrency.book.concurrency.log
import org.apache.commons.io.FileUtils.{iterateFiles, listFiles}

import java.io._
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

/**
 * @Author: JMD
 * @Date: 4/4/2023
 */
object FuturesClumsyCallback {
  private def blacklistFile(name: String): Future[List[String]] = Future{
    val f = Source.fromFile(name)
    try{
      val blackFiles = f.getLines().filter(x => !x.startsWith("#") && x.nonEmpty).toList
      println("blackFiles is: ", blackFiles)
      blackFiles
    }finally {
      f.close()
    }
  }

  /**
   *
   */
  private def findFiles(patterns: List[String]): List[String] = {
    val root = new File(".")
    val ret = iterateFiles(root, null, true).asScala.toList.flatMap(f => {
      patterns.map(pat => {
        val absPath = root.getCanonicalPath + File.separator + pat
        println("#" * 60)
        println("absPath is: ", absPath)
        println("f.getCanonicalPath is: ", f.getCanonicalPath)
        if (f.getCanonicalPath.contains(absPath)) {
          println("@@@", f.getCanonicalPath)
          f.getCanonicalPath
        }
        else {
          null
        }
      })
    })
    println("ret: ", ret)
    ret match {
      case null => null
      case _ => ret.filter(e => e != null && e.nonEmpty)
    }
  }

  def blacklisted(name: String): Future[List[String]] = {
    blacklistFile(name).map(patterns => findFiles(patterns))
  }

  def main(args: Array[String]): Unit = {
    blacklistFile("Y:\\github\\BigDataRecord\\concurrency\\.gitignore")
      .foreach{
      case lines =>
        val files = findFiles(lines)
        if(files != null) log(s"match: ${files.mkString("\n")}")
    }
    Thread.sleep(1000)
  }
}
