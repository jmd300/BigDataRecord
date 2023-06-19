package com.zoo.flink.java.core;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import com.zoo.flink.java.util.UrlViewCount;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 5/15/2023

 * 在日常业务场景中，我们经常会开比较大的窗口来计算每个窗口的pv 或者 uv 等数据。但窗口开的太大，会使我们看到计算结果的时间间隔变长。所以我们可以
 * 使用触发器，来隔一段时间触发一次窗口计算。我们在代码中计算了每个 url 在 10 秒滚动窗口的 pv 指标，然后设置了触发器，每隔 1 秒钟触发一次窗口的计算。
 *
 * 如何判断十秒前窗口中的数据是否被释放掉了呢？还是在内存里？应该是释放了，trigger中没有调用释放，那就不在当前window后释放把，有待确认。
 */
public class TriggerDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp)
                )
                .keyBy(r -> r.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .process(new WindowResult())
                .print();

        env.execute();
    }
    public static class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> iterable, Collector<UrlViewCount> collector) throws Exception {
            collector.collect(
                    new UrlViewCount(
                            s,
                            // 获取迭代器中的元素个数
                            iterable.spliterator().getExactSizeIfKnown(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }
    public static class MyTrigger extends Trigger<Event, TimeWindow> {
        @Override
        public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
            if (isFirstEvent.value() == null) {
                for (long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // FIRE（触发）：触发计算，输出结果
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // CONTINUE（继续）：什么都不做
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
            isFirstEvent.clear();
        }
    }
}
