package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @Author: JMD
 * @Date: 3/17/2023
 */
object ReadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

  }
}
