package com.zoo.flink.scala.sink

import com.zoo.flink.scala.util.FlinkEnv
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.createTypeInformation

import java.util.concurrent.TimeUnit

/**
 * Author: JMD
 * Date: 5/12/2023
 */
object SinkToTxtFileDemo extends FlinkEnv{
  def main(args: Array[String]): Unit = {
    env.setParallelism(4)

    // 行编码： StreamingFileSink.forRowFormat（basePath， rowEncoder）。
    // 批量编码： StreamingFileSink.forBulkFormat（basePath， bulkWriterFactory）。
    val fileSink: StreamingFileSink[String] = StreamingFileSink.forRowFormat[String](new Path("./output"),
      new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024).build()
      )
      .build()

    // 将 Event 转换成 String 写入文件
    arrayStream.map(_.toString).addSink(fileSink)

    env.execute
  }
}
