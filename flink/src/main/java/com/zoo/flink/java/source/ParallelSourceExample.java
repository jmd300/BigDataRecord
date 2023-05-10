package com.zoo.flink.java.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
class CustomSource implements ParallelSourceFunction<Integer> {
    private boolean running = true;
    private final Random random = new Random();

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(random.nextInt());
            TimeUnit.MILLISECONDS.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
public class ParallelSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSource()).setParallelism(2).print();
        env.execute();
    }
}
