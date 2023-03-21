package com.zoo.spark.batch

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val argsMap = new ArgsMap(args)

    val inputPath = argsMap.getOrElse("inputPath", "C:\\Users\\GUOWE\\Desktop\\input.txt")
    val outPath = argsMap.getOrElse("outPath", "C:\\Users\\GUOWE\\Desktop\\out")

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val spark = new SparkContext(conf)

    spark.textFile(inputPath)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 4)
      .sortBy(_._2, ascending = false)
      .saveAsTextFile(outPath)

    spark.stop()
  }
}
