package com.zoo.spark.batch.dataframe

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import utils.ArgsMap
import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * Author: JMD
 * Date: 5/8/2023
 *
 * Parquet保存数据为Map， 有的博客说parquet不支持Map和Array，之前都用过，觉得不能啊！
 */
object OutputFormat {
  def main(args: Array[String]): Unit = {
    val argsMap = new ArgsMap(args)

    val inputPath = argsMap.getOrElse("inputPath", "input/words.txt")
    val outPath = argsMap.getOrElse("outPath", "output/")

    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    println(spark.sessionState)
    val df = spark.read.option("header", "false").csv(inputPath).toDF("word")
      .withColumn("map", map(col("word"), lit(1)))
    df.show()

    df.write.mode(SaveMode.Overwrite).parquet(outPath)

    spark.read.parquet(outPath).printSchema
  }
}
