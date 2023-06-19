package com.zoo.flink.java.operator;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Author: JMD
 * @Date: 6/19/2023
 * Stream FlatMap富函数的使用示例
 * 可以在富函数内使用  getRuntimeContext()  获取 runtimeContext，创建状态变量，再获取状态值，设置状态值初始值

 * 1 sourceStream必须要先keyBy然后才能使用Keyed State
 * 2 需要继承RichxxxxFunction才行，在open之前声明，在open中初始化，在算子方法中使用和处理。不能继承xxxxxFunction，因为没有open方法，无法初始化，会报错。
 * 3 open方法中只能初始化Keyed State，无法使用Keyed State（比如：获取值等操作），不然报错。因为open方法不属于Keyed上下文，算子方法才属于Keyed上下文
 */
public class RichFlatMapFunctionDemo extends FlinkEnv {

    public static class MyRichFunction extends RichFlatMapFunction<String, String> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration config) throws IOException {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("countState", Types.INT);
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws IOException {
            // 获得 state
            int count  = countState.value() == null ? 0 : countState.value();
            countState.update(++count);

            // 获得流中数据的计数
            out.collect(value + " : " + count);
        }
    }

    public static void main(String[] args) throws Exception {
        arrayStream.map(e -> e.user).keyBy(String::length).flatMap(new MyRichFunction()).print();
        env.execute();
    }
}
