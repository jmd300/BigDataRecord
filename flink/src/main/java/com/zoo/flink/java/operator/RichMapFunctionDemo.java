package com.zoo.flink.java.operator;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;


/**
 * @Author: JMD
 * @Date: 5/11/2023
 * Stream Map富函数的使用示例
 */
public class RichMapFunctionDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        /*
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
