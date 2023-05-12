package com.zoo.flink.java.source;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class SourceDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary","./home",1000L));
        clicks.add(new Event("Bob","./cart",2000L));

        // 从集合中获取DataStream
        DataStream<Event> stream1 = env.fromCollection(clicks);

        DataStream<Event> stream2 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream1.print();
        stream2.print();

        /**
         * 参数可以是目录，也可以是文件；
         * 路径可以是相对路径，也可以是绝对路径；
         * 相对路径是从系统属性 user.dir 获取路径: idea 下是 project 的根目录, standalone 模式下是集群节点根目录；
         * 也可以从 hdfs 目录下读取, 使用路径 hdfs://..., 由于 Flink 没有提供 hadoop 相关依赖,
         * 需要 pom 中添加相关依赖
         */
        DataStream<String> stream3 = env.readTextFile("input/words.txt");
        stream3.print();

        env.execute();
    }
}
