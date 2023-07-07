package com.zoo.flink.scala.table

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.eventtime.WatermarkStrategy.forMonotonousTimestamps
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssignerSupplier}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Author: JMD
 * Date: 7/7/2023
 */
object WindowTopNDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val eventStream1 = arrayStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(0)) {
      override def extractTimestamp(event: Event): Long = event.timestamp
    })

    val watermarkStrategy: WatermarkStrategy[Event] = WatermarkStrategy
      .forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(element: Event, recordTimestamp: Long): Long = element.timestamp
      })


    val eventStream2 = arrayStream.assignTimestampsAndWatermarks(watermarkStrategy)

    // 将数据流转换成表，并指定时间属性
    val eventTable: Table = tableEnv.fromDataStream(eventStream1,
      $("user"),
      $("url"),
      $("timestamp").rowtime.as("ts"))  // 将 timestamp 指定为事件时间，并命名为 ts

    // 为方便在 SQL 中引用，在环境中注册表 EventTable
    tableEnv.createTemporaryView("EventTable", eventTable)

    val subQuery =
      s"""
         |select window_start, window_end, user, count(url) as cnt
         |from table(
         |  tumble(table EventTable, descriptor(ts), interval '1' hour)
         |)
         |group by window_start, window_end, user
         |""".stripMargin


    // 执行 SQL 得到结果表, ROW_NUMBER是从1开始的
    // scala 中写sql更清晰，拼接变量也更方便
    val topNQuery =
      s"""
         |select *
         |from(
         |  select *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end order by cnt desc) as row_num
         |  from ($subQuery)
         |)
         |where row_num <= 2
         |""".stripMargin
    val result = tableEnv.sqlQuery(topNQuery)

    tableEnv.toDataStream(result).print

    env.execute()
  }
}
