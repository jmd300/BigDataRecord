package com.zoo.flink.java.core;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * @Author: JMD
 * @Date: 5/12/2023
 *
 * onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。
 */
public class LatenessWaterMarkDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        env.addSource(new ClickSource())
        .assignTimestampsAndWatermarks(                                                     // 插入水位线的逻辑
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))    // 针对乱序流插入水位线，延迟时间设置为 5s
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                // 抽取时间戳的逻辑
                                return element.timestamp;
                            }
                        })
                )
                .print();
        /*
         * 有序流的水位线生成器本质上和乱序流是一样的，相当于延迟设为 0 的乱序流水 位线生成器，两者完全等同：
         * WatermarkStrategy.forMonotonousTimestamps()
         * WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
         */


    }
}
