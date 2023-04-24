package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Parameters

object HeapMemoryAndMaxMemory {
  private def printLocal: Unit = {
    val inMB = 1024 * 1024
    val totalMemory = Runtime.getRuntime().totalMemory() / inMB
    val maxMemory = Runtime.getRuntime().maxMemory() / inMB
    val freeMemory = Runtime.getRuntime().freeMemory() / inMB
    println("##MB: ", totalMemory, maxMemory, freeMemory)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.executor.cores", "1")
      .set("spark.driver.memory", "1g")
      .set("spark.executor.memory", "1g")
      .set("spark.executor.instances", "1")
      .setMaster("local[1]")
      .setAppName("HeadMemoryAndMaxMemory")

    val spark = SparkSession.builder().config(conf)
      .getOrCreate()

    import spark.implicits._
    val parameters = new Parameters(spark)
    val maxMemory = Runtime.getRuntime.maxMemory
    println("maxMemory: ", maxMemory)
    printLocal
    println("executorNum is ", parameters.getExecutorNum)
    println("executor Core is ", parameters.getExecutorCores)
    println("executor memory is ", parameters.getExecutorMemory())

    val arr = Array(1, 2, 3)
    val rdd = spark.sparkContext.makeRDD(arr)

    rdd.toDF().show
    while(true){
      Thread.sleep(1000)
    }
  }
}
