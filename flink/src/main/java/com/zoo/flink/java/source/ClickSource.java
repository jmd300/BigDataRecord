package com.zoo.flink.java.source;

import com.zoo.flink.java.util.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    Random random = new Random(); // 在指定的数据集中随机选取数据

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔 1 秒生成一个点击事件，方便观测
            TimeUnit.MILLISECONDS.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
