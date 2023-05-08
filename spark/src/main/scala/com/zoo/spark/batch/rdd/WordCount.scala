package com.zoo.spark.batch.rdd

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SaveMode
import utils.ArgsMap
import utils.Function.createLocalSparkSessionAndSparkContext

import java.util.concurrent.TimeUnit

object WordCount {
  def main(args: Array[String]): Unit = {
    val argsMap = new ArgsMap(args)

    val inputPath = argsMap.getOrElse("inputPath", "input/words.txt")
    val outPath = argsMap.getOrElse("outPath", "Y:\\out")

    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    println(spark.sessionState)
    // println("spark.memory.offHeap.enabled: ", spark.sparkContext.getConf.get("spark.memory.offHeap.enabled"))

    println(sc.getExecutorMemoryStatus)

    val ret = sc.textFile(inputPath)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 4)
      // .sortBy(_._2, ascending = false)
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
