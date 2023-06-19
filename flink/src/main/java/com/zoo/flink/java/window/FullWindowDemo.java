package com.zoo.flink.java.window;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @Author: JMD
 * @Date: 5/15/2023

 * 与增量聚合函数不同，全窗口函数需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。
 * 很明显，这就是典型的批处理思路了——先攒数据，等一批都到齐了再正式启动处理流程。
 * 这样做毫无疑问是低效的：因为窗口全部的计算任务都积压在了要输出结果的那一瞬间，而在之前收集数据的漫长过程中却无所事事。
 * 这就好比平时不用功，到考试之前通宵抱佛脚，肯定不如把工夫花在日常积累上。

 * 那为什么还需要有全窗口函数呢？这是因为有些场景下，我们要做的计算必须基于全部的数据才有效，这时做增量聚合就没什么意义了；
 * 另外，输出的结果有可能要包含上下文中的一些信息（比如窗口的起始时间），这是增量聚合函数做不到的。
 * 所以，我们还需要有更丰富的窗口计算方式，这就可以用全窗口函数来实现。

 * WindowFunction 能提供的上下文信息较少，也没有更高级的功能。
 * 事实上，它的作用可以被 ProcessWindowFunction 全覆盖，所以之后可能会逐渐弃用。
 * 一般在实际应用，直接使用 ProcessWindowFunction 就可以了。
 */
public class FullWindowDemo extends FlinkEnv {
    // 使用全窗口函数做 sum，简单示例，不需要如此麻烦
    public static class MyWindowFunction implements WindowFunction<Event, String, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Event> input, Collector<String> out) throws Exception {
            int sum = 0;

            for (Event record : input) {
                sum += 1;
            }

            out.collect("Key: " + key + ", Window: " + window + ", sum: " + sum);
        }
    }

    public static void main(String[] args) {
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                //.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1)

                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(e -> e.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindowFunction());
    }
}
