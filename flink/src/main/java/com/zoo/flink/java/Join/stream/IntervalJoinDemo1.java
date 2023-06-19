package com.zoo.flink.java.Join.stream;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: JMD
 * @Date: 5/29/2023
 *
 * 间隔联结
 */
public class IntervalJoinDemo1 extends FlinkEnv {
    public static void main(String[] args) {
        arrayStream.keyBy(e -> e.user)
                .intervalJoin(arrayStream.keyBy(e -> e.user))
                .between(Time.milliseconds(-2), Time.milliseconds(1))
                .process (new ProcessJoinFunction<Event, Event, String>(){
                    @Override
                    public void processElement(Event left, Event right, ProcessJoinFunction<Event, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.user + right.user);
                    }
                });
    }
}
