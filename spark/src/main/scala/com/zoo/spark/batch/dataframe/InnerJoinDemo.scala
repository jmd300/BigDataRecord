package com.zoo.spark.batch.dataframe

import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * @Author: JMD
 * @Date: 4/24/2023
 */
object InnerJoinDemo {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

  }
}
