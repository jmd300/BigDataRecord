package com.zoo.spark.batch.dataframe

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * @Author: JMD
 * @Date: 4/24/2023
 */
object GroupByDemo {
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    val data: DataFrame = null

    val ret = data.groupBy("user")
      .pivot("")
      .agg(
        sum("num") as "user_num"
      )

  }
}
