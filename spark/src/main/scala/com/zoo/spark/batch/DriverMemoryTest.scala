package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lit}
import utils.Function.computeDataFrameSize

import java.util.concurrent.TimeUnit

/**
 * @Author: JMD
 * @Date: 4/7/2023
 */
object DriverMemoryTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.driver.memory", "40m")
      .set("spark.driver.maxResultSize", "500m")
      .setAppName(this.getClass.getName)
      .setMaster("local[12]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val dataDf = spark.read.csv("input/words.txt").repartition(12).cache()
    println(dataDf.schema)
    println("dataDf.rdd.getNumPartitions is: ", dataDf.rdd.getNumPartitions)

    println("dataDf size is: ", computeDataFrameSize(spark, dataDf))

    // 100 多些
    val arr = (0 until 50 * 14000).toArray
    val dataSetDf = dataDf.withColumn("arr", lit(arr))
      .withColumn("arr", explode(col("arr")))


    println("dataSetDf: ", dataSetDf.collect())

    println("DataSetDf size is: ", computeDataFrameSize(spark, dataSetDf))
    while(true) {
      TimeUnit.SECONDS.sleep(5)
      println("sleep...")
    }
  }
}
