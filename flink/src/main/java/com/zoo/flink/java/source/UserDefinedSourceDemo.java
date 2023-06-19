package com.zoo.flink.java.source;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class UserDefinedSourceDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 有了自定义的 source function，调用 addSource 方法
        // 这里要注意的是 SourceFunction 接口定义的数据源，并行度只能设置为 1，如果数据源设
        // 置为大于 1 的并行度，则会抛出异常。
        // 所以如果我们想要自定义并行的数据源的话，需要使用 ParallelSourceFunction
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("UserDefinedSourceDemo");

        env.execute();
    }
}
