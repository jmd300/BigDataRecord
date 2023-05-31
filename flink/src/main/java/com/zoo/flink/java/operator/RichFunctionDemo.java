package com.zoo.flink.java.operator;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;



/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class RichFunctionDemo extends FlinkEnv {
    /**
     * 可以在富函数内使用  getRuntimeContext()  获取 runtimeContext，创建状态变量，再获取状态值，设置状态值初始值
     */
    public static class MyRichFunction extends RichFlatMapFunction<String, String> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration config) throws IOException {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("countState", Types.INT);
            RuntimeContext runtimeContext = getRuntimeContext();
            countState = runtimeContext.getState(descriptor);

            Integer currentValue = countState.value();

            if (currentValue == null) {
                // 如果当前状态值为空，则设置默认值
                countState.update(0);
            }
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws IOException {
            // access state
            Integer count = countState.value();
            count++;
            countState.update(count);

            out.collect(value + " : " + count);
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * 将点击事件转换成长整型的时间戳输出
         * 富函数类提供了 getRuntimeContext()方法，可以获取到运行时上下文的一些信息，例如程序执行的并行度，任务名称，以及状态（state）。
         */
        arrayStream.map(new RichMapFunction<Event, Long>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println(" 索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
                    }
                    @Override
                    public Long map(Event value) throws Exception {
                        return value.timestamp;
                    }
                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println(" 索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
                    }
                })
                .print();
        env.execute();
    }
}
