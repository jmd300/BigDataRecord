package com.zoo.flink.java.process;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 5/23/2023
 */
public class ProcessWindowFunctionDemo extends FlinkEnv {
    public static void main(String[] args) {
        arrayStream.keyBy(data -> data.user) // 按照 url 进行分组
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) // 时间窗口为 5 分钟
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

                    }
                });
    }
}
