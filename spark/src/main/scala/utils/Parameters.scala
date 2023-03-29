package utils

import org.apache.spark.sql.SparkSession

/**
 * @Author: JMD
 * @Date: 3/23/2023
 */
class Parameters(spark: SparkSession) {
  private val executorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus

  def getExecutorCores: Int = {
    spark.conf.get("spark.executor.cores").toInt
  }

  def getExecutorNum: Int = {
      // spark.sparkContext.getExecutorMemoryStatus.size
    executorMemoryStatus.size
  }

  def getExecutorMemory(): String = {
    executorMemoryStatus.values.head._2.toString
  }
}
