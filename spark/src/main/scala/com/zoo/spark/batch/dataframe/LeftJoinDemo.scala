package com.zoo.spark.batch.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utils.Function.computeDataFrameSize

/**
 * Author: JMD
 * Date: 4/24/2023
 *
 * DataFrame API 中只支持少量的 JOIN hints，例如 BROADCAST、MERGE 和 SHUFFLE_HASH。
 * 如果需要使用其他类型的 JOIN hints，可以将 DataFrame 转换为临时视图，然后在 SQL 查询中使用 JOIN hints。
 * 有关 join hints sql 中的关键字和语法：https://spark.apache.org/docs/3.2.0/sql-ref-syntax-qry-select-hints.html#join-hints
 */
object LeftJoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster(s"local[6]")

    val spark = SparkSession.builder()
      .config(conf)
      .config("spark.sql.autoBroadcastJoinThreshold", "1048576b")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val userInfoDf = spark.read.option("header", "true").csv("input/userInfo").cache
    userInfoDf.createTempView("userInfoDfTable")

    userInfoDf.show(1)
    println("userInfoDfSize is: ", computeDataFrameSize(spark, userInfoDf))

    val userLogDf: DataFrame = spark.read.option("header", "true").csv("input/userLog").cache
    userLogDf.createTempView("userLogDfTable")

    userLogDf.show(1)
    println("userLogDf is: ", computeDataFrameSize(spark, userLogDf))

    // 输出的默认值为： 10485760b 也就是10MB
    val threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    println(s"Current broadcast join threshold is: $threshold")

    // 1 LEFT JOIN / DataFrame JOIN
    // 1.1 broadcast hash join
    val dataFrameBroadcastJoinRetDf1 = userLogDf.join(userInfoDf, Seq("userId"), "left")

    // autoBroadcastJoinThreshold为默认值10MB时：explain显示 BroadcastHashJoin
    // autoBroadcastJoinThreshold为默认值1 MB时：explain显示 SortMergeJoin
    dataFrameBroadcastJoinRetDf1.explain()
    val dataFrameBroadcastJoinRetDf2 = userLogDf.join(broadcast(userInfoDf), Seq("userId"), "left")
    // 即使 autoBroadcastJoinThreshold 小于基表在内存中的大小 explain显示 BroadcastHashJoin，证明了broadcast函数的作用
    dataFrameBroadcastJoinRetDf2.explain()

    // 以下都是BROADCAST JOIN
    val dataFrameBroadcastJoinRetDf1_1 = userLogDf.join(userInfoDf.hint("BROADCAST"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf1_1.explain()

    val dataFrameBroadcastJoinRetDf1_2 = userLogDf.join(userInfoDf.hint("BROADCASTJOIN"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf1_2.explain()

    val dataFrameBroadcastJoinRetDf1_3 = userLogDf.join(userInfoDf.hint("MAPJOIN"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf1_3.explain()

    // 以下都是Sort Merge Join
    val dataFrameBroadcastJoinRetDf2_1 = userLogDf.join(userInfoDf.hint("SHUFFLE_MERGE"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf2_1.explain()

    val dataFrameBroadcastJoinRetDf2_2 = userLogDf.join(userInfoDf.hint("MERGEJOIN"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf2_2.explain()

    val dataFrameBroadcastJoinRetDf2_3 = userLogDf.join(userInfoDf.hint("MERGE"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf2_3.explain()


    // 1.2 Shuffle Sort Merge JOIN 关键字：SHUFFLE_HASH
    // explain中也显示了 ShuffledHashJoin
    val dataFrameBroadcastJoinRetDf3_1 = userLogDf.join(userInfoDf.hint("SHUFFLE_HASH"), Seq("userId"), "left")
    dataFrameBroadcastJoinRetDf3_1.explain()
  }
}
