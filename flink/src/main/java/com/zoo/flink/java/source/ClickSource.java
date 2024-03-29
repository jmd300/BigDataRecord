package com.zoo.flink.java.source;

import com.zoo.flink.java.util.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class ClickSource implements SourceFunction<Event> {
    public ClickSource(){

    }
    int num;
    public ClickSource(int num){
        this.num = num;
    }
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    Random random = new Random(); // 在指定的数据集中随机选取数据

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        int count = 0;
        while (running && num > count++) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔 1 秒生成一个点击事件，方便观测
            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> eventDataStreamSource = env.addSource(new ClickSource(100))
                .map(new MapFunction<Event, String>() {

                    @Override
                    public String map(Event value) throws Exception {
                        return value.url + "," + value.user + "," + value.timestamp;
                    }
                });

        // 创建 StreamingFileSink，并指定序列化器和编码器
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path("out/csv/"), new SimpleStringEncoder<String>("UTF-8"))
                .build();

        eventDataStreamSource.addSink(sink);
        env.execute();
    }
}
