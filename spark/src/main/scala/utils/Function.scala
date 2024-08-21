package utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
 * Author: JMD
 * Date: 4/7/2023
 */
object Function {
  /**
   * 计算DataFrame占用内存大小
   *
   * @param spark     SparkSession
   * @param df        要计算占用内存大小的DataFrame
   * @param numerical 是否返回单位后缀
   * @return 占用内存大小
   */
  def computeDataFrameSize(spark: SparkSession, df: DataFrame, numerical: Boolean = false): String = {
    df.cache.count
    val plan = df.queryExecution.logical
    val estimated: BigInt = spark
      .sessionState
      .executePlan(plan)
      .optimizedPlan
      .stats
      .sizeInBytes

    val sizeMb = (estimated.toDouble / 1024.0 / 1024.0).formatted("%.4f").toDouble
    if (numerical) sizeMb.toString
    else {
      if (sizeMb < 1024) sizeMb.formatted("%.4f") + "MB"
      else (sizeMb / 1024).formatted("%.4f") + "GB"
    }
  }

  def createLocalSparkSessionAndSparkContext(coreNum: Int = 2): (SparkSession, SparkContext) = {
    /**
     * 在使用 YARN 作为集群管理器时，Spark 会自动忽略 setMaster 的设置，并根据提交命令中的 --master yarn 进行配置。
     */
    val conf = new SparkConf().setAppName("WordCount").setMaster(s"local[${coreNum.toString}]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    (spark, sc)
  }


  // 日期字符串格式：yyyyMMdd
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def dateDifference(dateStr1: String, dateStr2: String): Long = {
    // 将字符串转换为 LocalDate 对象
    val date1 = LocalDate.parse(dateStr1, formatter)
    val date2 = LocalDate.parse(dateStr2, formatter)

    // 计算日期之间的天数差值
    ChronoUnit.DAYS.between(date1, date2)
  }

  def main(args: Array[String]): Unit = {
    val date1 = "20231025"
    val date2 = "20231029"

    val daysBetween = dateDifference(date1, date2)
    println(s"Days between $date1 and $date2: $daysBetween")
  }
}
