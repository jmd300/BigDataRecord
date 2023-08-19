package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * Author: JMD
 * Date: 4/10/2023
 */
object PeekConfig {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    val shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions")
    println("shufflePartitions: ", shufflePartitions)

    val maxShuffledHashJoinLocalMapThreshold = spark.conf.get("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold")
    println("maxShuffledHashJoinLocalMapThreshold: ", maxShuffledHashJoinLocalMapThreshold)

  }

}
