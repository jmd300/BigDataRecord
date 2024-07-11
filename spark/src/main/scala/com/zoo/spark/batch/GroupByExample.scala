package com.zoo.spark.batch

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions.{ broadcast, col, udf}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

import org.apache.log4j.{Level, Logger} //导包


/**
 * 算过去 30 天有哪些用户是连续 7 天登录我们 APP 的， 如何写 SQL， 思路？
 * 1. 过滤掉不需要的数据，如30天内有某7天连续登录，则第，7，14，21，28其中某一天是必须要登录的。从这4天的数据中找出
 * 2.
 */
object GroupByExample {
  // array表示有序的日期字符串
  // flag 表示是否已经确认连续7天登录了，1则为确认，0则为尚未确认
  // 使用case class 对应，Encoders.product
  // 如果是普通class，merge中就无法收到reduce的结果。
  case class BufClass(array: Array[String], flag: Int) extends Serializable {
    override def toString: String = {
      "[BufClass Object] flag is: " + flag+ "---" + array.mkString(",")
    }
  }

  // 写一个聚合函数
  private class ComputeWeekLoginUser extends Aggregator[String, BufClass, Int] {
    def zero: BufClass = new BufClass(Array.empty, 0)

    // reduce输出数据的类型的编码
    def bufferEncoder: Encoder[BufClass] = Encoders.product

    // finish 最终输出的数据的类型的编码
    def outputEncoder: Encoder[Int] = Encoders.scalaInt

    private def computedFlag(dateArray: Array[String]): Int = {
      if(dateArray.isEmpty){
        return 0
      }

      var count = 1
      var lastDate = dateArray(0)

      dateArray.foreach(date => {
        if (date == lastDate) {}
        else if (date.replace("-", "").toInt - lastDate.replace("-", "").toInt == 1) {
          count += 1
        } else {
          count = 0
        }
        lastDate = date
      })

      if (count >= 7) 1 else 0
    }

    /**
     * @param b reduce 中间结果
     * @param a 日期
     * @return 中间结果
     */
    override def reduce(b: BufClass, a: String): BufClass = {

      // 这里可以用自定义函数写出更有效率的排序，保证数组有序，每次只添加一个元素即可
      val dateArray = (b.array :+ a).sortBy(date => date)

      val flag = if (b.flag == 1) {
        1
      }
      else {
        computedFlag(dateArray)
      }
      BufClass(dateArray, flag)
    }

    override def merge(b1: BufClass, b2: BufClass): BufClass = {
      // 这里可以用自定义函数写出更有效率的排序，保证数组有序，因为此时2个数组都是有序的
      val dateArray = (b1.array ++ b2.array).sortBy(date => date)

      val flag = if (Math.max(b1.flag, b2.flag) == 1) {
        1
      }
      else {
        computedFlag(dateArray)
      }
      BufClass(dateArray, flag)
    }
    override def finish(reduction: BufClass): Int = reduction.flag
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //只输出错误的日志信息

    val conf = new SparkConf().setMaster("local[1]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val computeWeekLoginUser = functions.udaf(new ComputeWeekLoginUser())
    spark.udf.register("compute_week_login_user", computeWeekLoginUser)

    // 示例数据
    val array = Array(("user_1", "2024-05-01"),
      ("user_1", "2024-05-02"),
      ("user_1", "2024-05-03"),
      ("user_1", "2024-05-04"),
      ("user_1", "2024-05-05"),
      ("user_1", "2024-05-06"),
      ("user_1", "2024-05-07"),
      ("user_2", "2024-05-03"), ("user_3", "2024-05-05"), ("user_2", "2024-05-07"), ("user_4", "2024-05-08"))

    val data: DataFrame = spark.createDataFrame(array).toDF("user_id", "login_date")

    // 至少包含的一个日志
    val mustDateArray = Array("2024-05-07", "2024-05-14", "2024-05-28", "2024-05-21")


    // 1. 筛选出这4天的user_id去重
    val userDf = data.where($"login_date".isin(mustDateArray: _*)).select("user_id").distinct.cache

    println("userDf大小为：", userDf.count)
    userDf.show()

    // personDataFrame.join(orderDataFrame, personDataFrame("id_person") === orderDataFrame("id_person"), "inner").show()
    //       .join(broadcast(curMapDf.select("note_id")), Seq("note_id"), "left_anti")
    val afterFilterDataDf = data.join(broadcast(userDf), Seq("user_id")).cache
    println("afterFilterDataDf is: ")
    afterFilterDataDf.show
    println("after partitionNum is: ", afterFilterDataDf.rdd.getNumPartitions)
    // 2. 使用自定义聚合函数
    val res = afterFilterDataDf.groupBy("user_id").agg(
      computeWeekLoginUser($"login_date") as "result"
    )
    println("res is: ")
    res.show
  }
}
