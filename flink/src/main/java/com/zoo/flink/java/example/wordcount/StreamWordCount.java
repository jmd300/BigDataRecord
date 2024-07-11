package com.zoo.flink.java.example.wordcount;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * @Author: JMD
 * @Date: 3/17/2023

 * 批处理针对每个单词，只会输出一个最终的统计个数；
 * 而在流处理的打印结果中， “hello”这个单词每出现一次，都会有一个频次统计数据输出。
 * 这是流处理的特点，数据逐个处理，每来一条数据就会处理输出一次。
 * 通过打印结果，可以清晰地看到单词“hello”数量增长的过程

 * Flink 是一个分布式处理引擎，所以我们的程序也是分布式运行的。
 * 在开发环境里，会通过多线程来模拟 Flink 集群运行。所以这里结果前的数字，其实就指示了本地执行的不同线程，对应着 Flink 运行时不同的并行资源。
 * 这样第一个乱序的问题也就解决了：既然是并行执行，不同线程的输出结果，自然也就无法保持输入的顺序了。

 * 这里显示的编号为 1~4，是由于运行电脑的 CPU 是 4 核，所以默认模拟的并行线程有 4 个。
 * 这段代码不同的运行环境，得到的结果会是不同的。关于 Flink 程序并行执行的数量，可以通过设定“并行度”（Parallelism）来进行配置
 * com.zoo.flink.java.example.wordcount.StreamWordCount
 */
public class StreamWordCount extends FlinkEnv {
    public void run(Supplier<DataStreamSource<String>> readFunc) throws Exception {
        DataStreamSource<String> lineDss = readFunc.get();

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDss
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 分组,求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOne.keyBy(t -> t.f0)
                .sum(1);
        // 打印
        result.print();
        // 执行
        env.execute();
    }
    public static void runBoundedStreamWordCount() throws Exception {
        StreamWordCount wordCount = new StreamWordCount();
        wordCount.run(() -> env.readTextFile("input/words.txt"));
    }
    public static void runStreamWordCount() throws Exception{
        StreamWordCount wordCount = new StreamWordCount();
        wordCount.run(() -> env.socketTextStream("hadoop102", 7777));
    }

    public static void main(String[] args) throws Exception {
        // runBoundedStreamWordCount();
        runStreamWordCount();
    }
}
