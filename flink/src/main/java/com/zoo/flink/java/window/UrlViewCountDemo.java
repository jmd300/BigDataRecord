package com.zoo.flink.java.window;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import com.zoo.flink.java.pojo.UrlViewCount;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 5/15/2023

 * 在网站的各种统计指标中，一个很重要的统计指标就是热门的链接；想要得到热门的 url，前提是得到每个链接的“热门度”。一般情况下，
 * 可以用url 的浏览量（点击量）表示热门度。我们这里统计 10 秒钟的 url 浏览量，每 5 秒钟更新一次；
 * 另外为了更加清晰地展示，还应该把窗口的起始结束时间一起输出。我们可以定义滑动窗口，并结合增量聚合函数和全窗口函数来得到统计结果。

 * 代码中用一个 AggregateFunction 来实现增量聚合，每来一个数据就计数加一；
 * 得到的结果交给 ProcessWindowFunction，结合窗口信息包装成我们想要的 UrlViewCount，最终输出统计结果。
 * 注： ProcessWindowFunction 是处理函数中的一种，后面我们会详细讲解。这里只用它来
 * 将增量聚合函数的输出结果包裹一层窗口信息。窗口处理的主体还是增量聚合，而引入全窗口函数又可以获取到更多的信息包装输出，
 * 这样的结合兼具了两种窗口函数的优势，在保证处理性能和实时性的同时支持了更加丰富的应用场景。
 */
public class UrlViewCountDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));

        // 需要按照 url 分组，开滑动窗口统计
        stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 同时传入增量聚合函数和全窗口函数
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
                .print();

        env.execute();
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long,
            Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 自定义窗口处理函数， 只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中只有一个元素，就是增量聚合函数的计算结果
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

}
