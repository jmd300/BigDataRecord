package com.zoo.spark.sql

import org.apache.spark.sql.expressions.Window

object HiveExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark: SparkSession = SparkSession.builder().appName("sparkhive")
      .master("yarn")
      .config("hive.metastore.uris", "thrift://hadoop102:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(s"""show tables;""").show

    spark.read.orc("hdfs://hadoop102:8020/user/hive/warehouse/employee/").show()

    val window = Window.rowsBetween(-4, 4)
    // 关闭会话
    spark.close()
  }
}
