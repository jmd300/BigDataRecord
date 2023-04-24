package com.zoo.spark.batch.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author: JMD
 * @Date: 4/8/2023
 */
object CoalesceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val rdd = spark.read.csv("input/words.txt").toDF("key").rdd.map(_.toString())
    println("rdd.getNumPartitions is: ", rdd.getNumPartitions)

    val rdd2 = rdd.coalesce(5)
    println("rdd2.getNumPartitions is: ", rdd2.getNumPartitions)

    val rdd3 = rdd2.map((_, 1))
    println("rdd3.getNumPartitions is: ", rdd3.getNumPartitions)

    val rdd4 = rdd3.reduceByKey(_ + _)
    println("rdd4.getNumPartitions is: ", rdd4.getNumPartitions)

    val ret = rdd4.collect

    ret.foreach(println)
  }
}
