package com.zoo.spark.batch

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import utils.ArgsMap

import java.util.concurrent.TimeUnit

object WordCount {
  def main(args: Array[String]): Unit = {
    TimeUnit.SECONDS.sleep(20);
    val argsMap = new ArgsMap(args)

    val inputPath = argsMap.getOrElse("inputPath", "C:\\Users\\GUOWE\\Desktop\\input.txt")
    val outPath = argsMap.getOrElse("outPath", "Y:\\out")

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println(spark.sessionState)
    // println("spark.memory.offHeap.enabled: ", spark.sparkContext.getConf.get("spark.memory.offHeap.enabled"))

    println(sc.getExecutorMemoryStatus)

    val ret = sc.textFile(inputPath)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 4)
      .sortBy(_._2, ascending = false)
      .toDF("word", "count")
      .select(col("word"), col("count") * 2 as "count")

    ret.show()

    ret.write.mode(SaveMode.Overwrite).csv(outPath)

    while (true){
      println("while...")
      TimeUnit.SECONDS.sleep(1)
    }
    spark.stop()

  }
}
