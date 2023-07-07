package com.zoo.flink.java.sink;

import com.zoo.flink.java.source.ClickSource;
import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * @Author: JMD
 * @Date: 6/26/2023
 */
public class SinkToParquetFileDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        DataStreamSource<Event> data = env.addSource(new ClickSource(10));


        StreamingFileSink<Event> sink = StreamingFileSink
                .forBulkFormat(new Path("out/parquet"), ParquetAvroWriters.forReflectRecord(Event.class))
                // withRollingPolicy
                .build();

        data.addSink(sink);

        // 执行任务
        env.execute("Parquet Sink Example");
    }
}
