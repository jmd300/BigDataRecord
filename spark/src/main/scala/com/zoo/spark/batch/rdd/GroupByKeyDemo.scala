package com.zoo.spark.batch.rdd

import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * Author: JMD
 * Date: 4/24/2023
 */
object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

  }
}
