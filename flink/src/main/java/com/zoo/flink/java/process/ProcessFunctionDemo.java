package com.zoo.flink.java.process;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 5/23/2023
 */
public class ProcessFunctionDemo extends FlinkEnv {
    public static void main(String[] args) {
        // ProcessFunction的泛型参数是, 输入数据类型，输出数据类型
        arrayStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                // 自定义输出操作
                if(value.user.length() < 20){
                    out.collect(value.user);

                }
            }
        });
    }
}
