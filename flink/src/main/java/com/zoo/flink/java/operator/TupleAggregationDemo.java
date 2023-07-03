package com.zoo.flink.java.operator;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @Author: JMD
 * @Date: 5/11/2023

 * 一个聚合算子，会为每一个key保存一个聚合的值，在Flink中我们把它叫作“状态”（state）。
 * 所以每当有一个新的数据输入，算子就会更新保存的聚合结果，并发送一个带有更新后聚合值
 * 的事件到下游算子。对于无界流来说，这些状态是永远不会被清除的，所以我们使用聚合算子，
 * 应该只用在含有有限个 key 的数据流上。
 */
public class TupleAggregationDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );
        stream.keyBy(r -> r.f0).sum(1).print();
        stream.keyBy(r -> r.f0).sum("f1").print();

        stream.keyBy(r -> r.f0).max(1).print();
        stream.keyBy(r -> r.f0).max("f1").print();

        stream.keyBy(r -> r.f0).min(1).print();
        stream.keyBy(r -> r.f0).min("f1").print();

        /*
         * minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是， min()只计
         * 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包
         * 含字段最小值的整条数据。
         */

        stream.keyBy(r -> r.f0).minBy(1).print();
        stream.keyBy(r -> r.f0).minBy("f1").print();

        stream.keyBy(r -> r.f0).maxBy(1).print();
        stream.keyBy(r -> r.f0).maxBy("f1").print();

        // 如果数据流的类型是 POJO 类，那么就只能通过字段名称来指定，不能通过位置来指定
        arrayStream.keyBy(e -> e.user).max("timestamp").print();

        env.execute();
    }
}
