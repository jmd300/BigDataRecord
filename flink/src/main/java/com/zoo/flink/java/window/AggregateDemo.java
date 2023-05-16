package com.zoo.flink.java.window;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.HashSet;

/**
 * @Author: JMD
 * @Date: 5/15/2023

 * AggregateFunction 可以看作是 ReduceFunction 的通用版本，这里有三种类型：输入类型 （IN）、累加器类型（ACC）和输出类型（OUT）。 输入类型 IN 就是输入流中元素的数据类型；
 * 累加器类型 ACC 则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类型了。

 * 接口中有四个方法：
 * ⚫ createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚
 * 合任务只会调用一次。
 * ⚫ add()：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进
 * 一步聚合的过程。方法传入两个参数：当前新到的数据 value，和当前的累加器
 * accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之
 * 后都会调用这个方法。
 * ⚫ getResult()：从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，
 * 然后再基于这些聚合的状态计算出一个结果进行输出。比如之前我们提到的计算平均
 * 值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终
 * 结果。这个方法只在窗口要输出结果时调用。
 * ⚫ merge()：合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在
 * 需要合并窗口的场景下才会被调用；最常见的合并窗口（Merging Window）的场景
 * 就是会话窗口（Session Windows）。
 * 所以可以看到， AggregateFunction 的工作原理是：首先调用 createAccumulator()为任务初
 * 始化一个状态(累加器)；而后每来一个数据就调用一次 add()方法，对数据进行聚合，得到的
 * 结果保存在状态中；等到了窗口需要输出时，再调用 getResult()方法得到计算结果。很明显，
 * 与 ReduceFunction 相同， AggregateFunction 也是增量式的聚合；而由于输入、中间状态、输
 * 出的类型可以不同，使得应用更加灵活方便。
 *
 * 通过自定义数据源，统计滚动窗口下平均PV
 */
public class AggregateDemo extends FlinkEnv {
    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }
        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            accumulator.f0.add(value.user);
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }
        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }
        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));
        // 所有数据设置相同的 key，发送到同一个分区统计 PV 和 UV，再相除
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),
                        Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();
        env.execute();
        /**
         * 代码中我们创建了事件时间滑动窗口，统计 10 秒钟的“人均 PV”，每 2 秒统计一次。由于聚合的状态还需要做处理计算，因此窗口聚合时使用了更加灵活的 AggregateFunction。
         * 为了、统计 UV，我们用一个 HashSet 保存所有出现过的用户 id，实现自动去重；而 PV 的统计则类似一个计数器，每来一个数据加一就可以了。
         * 所以这里的状态，定义为包含一个 HashSet 和一个 count 值的二元组（Tuple2<HashSet<String>, Long>），每来一条数据，就将 user 存入 HashSet，
         * 同时 count 加 1。这里的 count 就是 PV，而 HashSet 中元素的个数（size）就是 UV；所以最终窗口的输出结果，就是它们的比值。
         * 这里没有涉及会话窗口，所以 merge()方法可以不做任何操作。
         * 通过 ReduceFunction 和 AggregateFunction 我们可以发现，增量聚合函数其实就是在用流处理的思路来处理有界数据集，核心是保持一个聚合状态，当数据到来时不停地更新状态。
         * 这就是 Flink 所谓的“有状态的流处理”，通过这种方式可以极大地提高程序运行的效率，所以在实际应用中最为常见。
         */
    }
}
