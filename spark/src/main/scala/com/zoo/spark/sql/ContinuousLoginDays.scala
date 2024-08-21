package com.zoo.spark.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.control.Breaks.{break, breakable}

object ContinuousLoginDays {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local")
      .getOrCreate()

    import spark.implicits._

    // t_user_attendance表的数据，用户登录情况
    // TODO: 1. 计算所有用户当前连续登录天数
    // TODO: 2. 计算所有用户历史最大连续登录天数
    val df = spark.createDataFrame(Seq(
      ("20200101", "A", 1),
      ("20200102", "A", 1),
      ("20200103", "A", 0),
      ("20200104", "A", 1),

      ("20200101", "B", 1),
      ("20200102", "B", 1),
      ("20200103", "B", 1),
      ("20200104", "B", 0),
    )) toDF("login_date", "uid", "is_sign_in")

    df.createTempView("t_user_attendance")

    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW temp as
         |select uid,
         |       login_date,
         |       is_sign_in,
         |       row_num - sum(is_sign_in) OVER (PARTITION BY uid ORDER BY login_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
         |  from(
         |    select uid,
         |           concat(substring(login_date, 1, 4), '-', substring(login_date, 5, 2), '-', substring(login_date, 7, 2)) as login_date,
         |           is_sign_in,
         |           row_number() over(partition by uid order by login_date) as row_num
         |    from t_user_attendance
         |    order by uid,login_date
         |  )
         |""".stripMargin
    )

    println("temp")

    spark.sql(s"""select * from temp""").show()

    // 假如没有标识未登录的数据行，则使用日期减去row_num的方式，然后分组聚合

    // 假如有标识未登录的数据行
    // 用户当前连续登录天数
    spark.sql(
      s"""
         |create TEMPORARY VIEW login_count AS
         |select uid,
         |       grp,
         |       sum(is_sign_in) as login_count,
         |       MIN(login_date) AS start_date,
         |       MAX(login_date) AS end_date
         |from temp
         |group by uid, grp
         |""".stripMargin)
    println("loginCount")
    spark.sql("select * from login_count").show()

    // 最大连续登录天数
    val maxLoginCount = spark.sql(
      s"""
         |select *
         |from (
         |  SELECT
         |      *,
         |      RANK() OVER (PARTITION BY uid ORDER BY login_count DESC) AS rank
         |  FROM login_count
         |)
         |WHERE rank = 1
         |""".stripMargin)
    maxLoginCount.show()

    // 使用spark编程

    def sumBeforeFirstZero(numList: Seq[Int]): Int = {
      for((num, index) <- numList.zipWithIndex){
        if(num == 0){
          return index
        }
      }
      numList.length
    }

    // 计算当前连续登录天数
    val computeCurrentContinuousLoginDays: UserDefinedFunction = udf((dateLoginList: Seq[String]) => {
      val numList = dateLoginList.map(e => {
        val arr = e.split("#")
        (arr.head, arr(1).toInt)
      }).sortBy(_._1).reverse.map(_._2)

      sumBeforeFirstZero(numList)
    })

    // 计算最大连续登录的天数
    val computeMaxContinuousLoginDays: UserDefinedFunction = udf((dateLoginList: Seq[String]) => {
      val numList = dateLoginList.map(e => {
        val arr = e.split("#")
        (arr.head, arr(1).toInt)
      }).sortBy(_._1).reverse.map(_._2)


      var maxLoginDays: Int = 0

      var sum = 0
      breakable{
        for ((num, index) <- numList.zipWithIndex) {
          if (num != 0) {
            sum += 1
          }
          else {
            if(sum != 0){
              maxLoginDays = Math.max(maxLoginDays, sum)
              sum = 0
              // 剩余的数量肯定少于已经出现的最大登录天数
              if (numList.length - index - 1 < maxLoginDays) {
                break()
              }
            }
          }
        }
      }
      maxLoginDays = Math.max(maxLoginDays, sum)
      maxLoginDays
    })

    df.printSchema()
    val data1 = df.withColumn("date_with_is_sign_in", concat(col("login_date"), lit("#"), col("is_sign_in")))
    data1.show()

    val data2 = data1.select("uid", "date_with_is_sign_in")
      .groupBy("uid")
      .agg(
        collect_list("date_with_is_sign_in") as "date_with_is_sign_in_list"
      )
      .withColumn("current_login_days", computeCurrentContinuousLoginDays(col("date_with_is_sign_in_list")))
      .withColumn("max_login_days", computeMaxContinuousLoginDays(col("date_with_is_sign_in_list")))

    data2.show(truncate = false)

    // 连续登录n天
  }
}
