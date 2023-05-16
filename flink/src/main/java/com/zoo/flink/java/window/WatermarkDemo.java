package com.zoo.flink.java.window;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * @Author: JMD
 * @Date: 5/15/2023

 * 会发现，当最后输入[Alice, ./prod?id=300, 15000]时， 流中会周期性地（默认 200毫秒） 插入一个时间戳为 15000L – 5 * 1000L – 1L = 9999 毫秒的水位线，已经到达了窗口
 * [0,10000)的结束时间，所以会触发窗口的闭合计算。而后面再输入一条[Alice, ./prod?id=200,9000]时，将不会有任何结果； 因为这是一条迟到数据， 它所属于的窗口已经触发计算然后销
 * 毁了（窗口默认被销毁），所以无法再进入到窗口中，自然也就无法更新计算结果了。
 * 窗口中的迟到数据默认会被丢弃，这会导致计算结果不够准确。 Flink 提供了有效处理迟到数据的手段。
 */
public class WatermarkDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 将数据源改为 socket 文本流，并转换成 Event 类型
        // 抽取时间戳的逻辑
        env.socketTextStream("hadoop102", 7777)
                .map((MapFunction<String, Event>) value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                // 插入水位线的逻辑
                // 针对乱序流插入水位线，延迟时间设置为 5s
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                )
                // 根据 user 分组，开窗统计
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new WatermarkDemoResult())
                .print();

        env.execute();
    }
    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkDemoResult extends ProcessWindowFunction<Event, String, String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long currentWatermark = context.currentWatermark();
            long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素， 窗口闭合计算时，水位线处于： " + currentWatermark);
        }
    }
}
