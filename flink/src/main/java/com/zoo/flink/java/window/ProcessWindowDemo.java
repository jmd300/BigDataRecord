package com.zoo.flink.java.window;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
/**
 * @Author: JMD
 * @Date: 5/15/2023
 *
 * 统计每小时 UV
 */
public class ProcessWindowDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        // 将数据全部发往同一分区，按窗口统计 UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();
        env.execute();
    }
    // 自定义窗口处理函数
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            // 遍历所有数据，放到 Set 里去重
            for (Event event: elements){
                userSet.add(event.user);
            }

            // 结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(" 窗 口 : " + new Timestamp(start) + " ~ " + new Timestamp(end) + " 的独立访客数量是： " + userSet.size());
        }
    }
}
