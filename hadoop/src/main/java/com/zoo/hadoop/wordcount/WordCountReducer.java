package com.zoo.hadoop.wordcount;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @Author: JMD
 * @Date: 5/5/2023
 */

/*
 * Text 		-  输入的键（即Mapper阶段输出的键）
 * IntWritable 	- 输入的值(个数)(即Mapper阶段输出的值)
 * Text 		- 输出的键
 * IntWritable 	- 输出的值
 * */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 对于所有 (key, value)会将相同的key的数据集合执行该reduce操作
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 1、统计键对应的个数
        int sum = 0;
        for(IntWritable value : values) {
            sum = sum + value.get();
        }

        // 2、设置reducer的输出
        IntWritable v = new IntWritable();
        v.set(sum);
        context.write(key, v);
    }
}
