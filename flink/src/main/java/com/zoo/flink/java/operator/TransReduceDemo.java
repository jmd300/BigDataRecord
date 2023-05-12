package com.zoo.flink.java.operator;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import com.zoo.flink.java.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class TransReduceDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 得到当前访问量最大的用户
        env.addSource(new ClickSource())
                // 将 Event 数据类型转换成元组类型
                .map((MapFunction<Event, Tuple2<String, Long>>) e -> Tuple2.of(e.user, 1L))
                // 使用用户名来进行分流
                .keyBy(r -> r.f0)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> {
                    // 每到一条数据，用户 pv 的统计值加 1
                    return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                })
                // 为每一条数据分配同一个 key，将聚合结果发送到一条流中 去
                .keyBy(r -> true)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> {
                    // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                    return value1.f1 > value2.f1 ? value1 : value2;
                })
                .print();

        env.execute();
    }
}
