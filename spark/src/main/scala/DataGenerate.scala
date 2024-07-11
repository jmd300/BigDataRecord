import org.apache.spark.rdd.RDD
import utils.Function.createLocalSparkSessionAndSparkContext

import scala.util.Random

/**
 * Author: JMD
 * Date: 4/24/2023
 *
 * 在 Spark 中，当将 DataFrame 保存为 CSV 文件时，Spark 会在相同目录下生成一个以 .crc 结尾的文件。
 * 这个 .crc 文件是用来校验 CSV 文件完整性的。CRC 全称"Cyclic Redundancy Check"， 即循环冗余校验，它是一种数据校验方法，通常用于检测数据传输或储存中的错误。在 Spark 中，
 * 这个 .crc 文件是通过计算 CSV 文件的 CRC 校验值生成的，如果 CSV 文件被修改，其 CRC 值也将随之改变，从而在后续使用过程中可以及时发现和修复数据的错误和损坏。
 * 生成 .crc 文件是在保存 CSV 文件之后的阶段进行的，是由 Spark 内部的文件系统（例如 Hadoop 的 HDFS）完成的。
 * 保存 CSV 文件后，Spark 将计算出其 CRC 校验值，并将其写入 .crc 文件中，以供后续校验使用。
 */
object DataGenerate {
  private val random = new Random()

  private def generateRandomLetter(): Char = {
    random.nextInt(1) match {
      case 0 => (random.nextInt(26) + 97).toChar
      case _ => (random.nextInt(26) + 65).toChar
    }
  }
  private def generateRandomString(len: Int): String = {
    (0 until len).map(_ => generateRandomLetter()).mkString("")
  }

  def main(args: Array[String]): Unit = {
    val (spark, sc) = createLocalSparkSessionAndSparkContext()
    import spark.implicits._
    val userIds = (0 until 10 * 10000).map(_ => generateRandomString(2)).toArray
    val itemIds = (0 until 50 * 10000).map(_ => generateRandomString(3)).toArray

    val actions = Array("view", "click", "like", "comment", "follow", "forward", "dislike", "collect")

    val userLogArray = (0 until 100 * 10000).map(_ => {
      (userIds(random.nextInt(userIds.length)), itemIds(random.nextInt(itemIds.length)), actions(random.nextInt(actions.length)))
    })

    println(userLogArray.take(5).mkString("Array(", ", ", ")"))
    val userLogRdd: RDD[(String, String, String)] = sc.parallelize(userLogArray, 4)

    println("userLogRdd 分区数量", userLogRdd.getNumPartitions)

    userLogRdd.toDF("userId", "itemId", "action")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("input/userLog/")

    // 用户名， 用户年龄
    val userInfoArray = userIds.map(e => (e, random.nextInt(20) + 10)).toSeq
    val userInfoRdd: RDD[(String, Int)] = sc.parallelize(userInfoArray, 12)


    // userInfoRdd.coalesce(2, shuffle = true)

    println("userInfoRdd 分区数量", userInfoRdd.getNumPartitions)
    userInfoRdd.toDF("userId", "duration")
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("input/userInfo/")
  }
}
