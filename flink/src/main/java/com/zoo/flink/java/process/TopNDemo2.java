package com.zoo.flink.java.process;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import com.zoo.flink.java.util.UrlViewCount;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import java.util.Comparator;
/**
 * @Author: JMD
 * @Date: 5/23/2023
 *
 * 作为process函数的示例，其实topN不需要如此冗余的实现，只需要 ProcessWindowFunction 即可
 */

public class TopNDemo2 extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 从自定义数据源读取数据
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        // 需要按照 url 分组，求出每个 url 的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream.keyBy(data -> data.url)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                        .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 对结果中同一个窗口的统计数据，进行排序处理
        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                // 聚合之后的数据的水位线是什么呢？
                // 在数据聚合过程中，当一个窗口执行结束后，聚合后的数据会被输出。此时，输出的数据所包含的水位线也会改变，变为聚合窗口中最迟的数据的事件时间戳
                .process(new TopN(2));
        result.print("result");
        env.execute();
    }

    // 自定义增量聚合
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {
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

    // 自定义全窗口函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    // 自定义处理函数，排序取 top n
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 将 n 作为属性
        private final Integer n;
        // 定义一个列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将 count 数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);
            // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
            // 可以不用加1，因为窗口内数据的时间戳是小于窗口结束时间戳的
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }
            // 清空状态，释放资源
            urlViewCountListState.clear();
            // 排序
            urlViewCountArrayList.sort((o1, o2) -> o2.count.intValue() - o1.count.intValue());

            // 取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间： ").append(new Timestamp(timestamp - 1)).append("\n");

            for (int i = 0; i < this.n; i++) {
                UrlViewCount UrlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i + 1) + " "
                        + "url： " + UrlViewCount.url + " "
                        + "浏览量： " + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }
}