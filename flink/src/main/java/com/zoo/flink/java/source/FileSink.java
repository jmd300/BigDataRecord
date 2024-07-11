package com.zoo.flink.java.source;

import com.zoo.flink.java.util.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class FileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> eventDataStreamSource = env.addSource(new ClickSource(100))
                .map(new MapFunction<Event, String>() {

                    @Override
                    public String map(Event value) throws Exception {
                        return value.url + "," + value.user + "," + value.timestamp;
                    }
                });

        // 创建 StreamingFileSink，并指定序列化器和编码器
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("out/csv/"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                ).build();

        eventDataStreamSource.addSink(sink);
        env.execute();
    }
}
