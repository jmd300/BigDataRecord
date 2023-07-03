package com.zoo.flink.scala.sink

import com.zoo.flink.java.util.Event
import com.zoo.flink.scala.source.ClickSource
import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.core.fs.Path

/**
 * Author: JMD
 * Date: 6/26/2023
 */
object SinkToParquetFileDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    val data: DataStream[Event] = env.addSource(new ClickSource(10))

    val streamParquetSink: StreamingFileSink[Event] = StreamingFileSink
      .forBulkFormat(new Path("out/parquet"), ParquetAvroWriters.forReflectRecord(classOf[Event]))
      // withRollingPolicy
      .build()

    data.addSink(streamParquetSink)

    // 执行任务
    env.execute("Parquet Sink Example")
  }
}
