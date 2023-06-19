package com.zoo.flink.java.sink;

import com.zoo.flink.java.util.Event;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;

import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class SinkToFileDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        env.setParallelism(4);

        StreamingFileSink<String> fileSink = StreamingFileSink.
                <String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 将 Event 转换成 String 写入文件
        arrayStream.map(Event::toString).addSink(fileSink);
        env.execute();
    }
}
