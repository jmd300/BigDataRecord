package com.zoo.spark.batch.rdd

import org.apache.spark.rdd.RDD
import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * Author: JMD
 * Date: 4/24/2023
 */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    val rdd:RDD[String] = null

    
  }
}
