package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Author: JMD
 * Date: 3/17/2023
 */
object ReadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadData").setMaster(s"local[6]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val hiveDf = spark.sql("select * from employee")
  }
}
