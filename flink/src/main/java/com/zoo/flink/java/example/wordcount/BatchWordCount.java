package com.zoo.flink.java.example.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 3/16/2023
 * 批处理接口运行word count

 * 命令行提交参数
 * bin/flink run -m hadoop102:8081 -c com.zoo.flink.wordcount.BatchWordCount ~/BigDataRecord-jar-with-dependencies.jar ../hadoop-3.1.3/wcinput hdfs://hadoop102:8020/out
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据 按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDs = env.readTextFile("input/words.txt").setParallelism(1);

        // 转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDs.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                //当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // word 进行分组、聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOne.setParallelism(2).groupBy(0).sum(1);

        // 打印结果
        sum.print();
        /*sum.map(e -> {
            System.out.println((e.f0 + "===" +  e.f1));
            return e;
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .writeAsCsv(args[1], WriteMode.OVERWRITE).setParallelism(2);*/

        // 可以直接链式编程
        /*
        env.readTextFile("input/words.txt")
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .groupBy(0).sum(1)
                .print();
                */
        // env.execute();
    }
}
