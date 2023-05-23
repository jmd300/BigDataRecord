package com.zoo.flink.java.process;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author: JMD
 * @Date: 5/23/2023

 * 为了实现数据的聚合统计，或者开窗计算之类的功能，一般都要先用 keyBy 算子对数据流进行“按键分区”，得到一个 KeyedStream。也就是指定一个键（key），
 * 按照它的哈希值（hash code）将数据分成不同的“组”，然后分配到不同的并行子任务上执行计算；这相当于做了一个逻辑分流的操作，从而可以充分利用并行计算的优势实时处理海量数据。
 * 只有在 KeyedStream 中才支持使用 TimerService 设置定时器的操作。所以一般情况下，是先做 keyBy 分区之后，再去定义处理操作；
 */
public class KeyedProcessFunctionDemo extends FlinkEnv {
    static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {
        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            long currTs = ctx.timerService().currentProcessingTime();
            out.collect("数据到达，到达时间： " + new Timestamp(currTs));
            // 注册一个 10 秒后的定时器
            ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("定时器触发，触发时间： " + new Timestamp(timestamp));
        }
    }
    public static void main(String[] args) {
        //  处理时间语义，不需要分配时间戳和 watermark
        arrayStream.keyBy(e -> e.user)
                .process(new MyKeyedProcessFunction())
                .print();

    }
}
