package com.zoo.spark.batch

import org.apache.spark.sql.functions._
import utils.Function.createLocalSparkSessionAndSparkContext

/**
 * Author: JMD
 * Date: 4/10/2023
 */
object SpecifyJoinType{
  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._

    // 构造两个DataFrame，分别为 leftDf 和 rightDf
    val leftDf = Seq((1, "A"), (2, "B"), (3, "C")).toDF("key", "value")
    val rightDf = Seq((1, "X"), (2, "Y"), (4, "Z")).toDF("key", "value")


    println("#" * 20, "Broadcast Join", "#" * 20)
    val broadcastJoinDf = leftDf.join(broadcast(rightDf), Seq("key"), "inner")
    broadcastJoinDf.explain()
    broadcastJoinDf.show

    println("#" * 20, "Hash Join", "#" * 20)
    val shuffleHashJoinDf = leftDf.join(rightDf.hint("shuffle_hash"), Seq("key"), "inner")
    shuffleHashJoinDf.explain()
    shuffleHashJoinDf.show

    println("#" * 20, "Sort Merge Join", "#" * 20)
    val shuffleSortMergeJoinDf = leftDf.join(rightDf.hint("merge"), Seq("key"), "inner")
    shuffleSortMergeJoinDf.explain()
    shuffleSortMergeJoinDf.show

    println("#" * 20, "Shuffle and Replicate Nested Loop Join", "#" * 20)
    val shuffleAndReplicateNestedLoopJoinDf = leftDf.join(rightDf.hint("shuffle_replicate_nl"), Seq("key"), "inner")
    shuffleAndReplicateNestedLoopJoinDf.explain()
    shuffleAndReplicateNestedLoopJoinDf.show

  }
}
