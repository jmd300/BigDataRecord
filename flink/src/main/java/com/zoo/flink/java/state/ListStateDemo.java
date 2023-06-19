package com.zoo.flink.java.state;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
/**
 * @Author: JMD
 * @Date: 6/5/2023

 * 根据一个key进行两个流的join的程序，同Flink Sql
 * SELECT * FROM A INNER JOIN B WHERE A.id = B.id
 */
public class ListStateDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env
                .fromElements(
                        Tuple3.of("a", "stream-1", 1000L),
                        Tuple3.of("b", "stream-1", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        // 提取时间戳和水位线
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2)
                );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                        Tuple3.of("a", "stream-2", 3000L),
                        Tuple3.of("b", "stream-2", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String,
                                        Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (t, l) -> t.f2)
                );

        stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    private ListState<Tuple3<String, String, Long>> stream1ListState;
                    private ListState<Tuple3<String, String, Long>> stream2ListState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        stream1ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING))
                        );
                        stream2ListState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple3<String, String, Long>>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING))
                        );
                    }
                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context context, Collector<String> collector) throws Exception {
                        stream1ListState.add(left);
                        for (Tuple3<String, String, Long> right : stream2ListState.get()) {
                            collector.collect(left + " => " + right);
                        }
                    }
                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        stream2ListState.add(right);
                        for (Tuple3<String, String, Long> left : stream1ListState.get()) {
                            collector.collect(left + " => " + right);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
