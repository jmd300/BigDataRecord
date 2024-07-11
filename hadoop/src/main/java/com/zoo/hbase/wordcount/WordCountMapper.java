package com.zoo.hbase.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author: JMD
 * @Date: 5/5/2023
 */

/*
 * LongWritable - 表示读取第几行
 * Text 		-  表示读取一行的内容
 * Text			- 表示输出的键
 * IntWritable 	- 表示输出的键对应的个数
 * */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text(); 					//键
    IntWritable v = new IntWritable(1); 	//值(值为1)

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // 输入的行编号 key就没有什么作用了

        // 1、获取读取一行的内容
        String line = value.toString();

        // 2、按空格切割读取的单词
        String[] words = line.split(" ");

        // 3、输出mapper处理完的内容
        for(String word : words) {
            // 给键设置值
            k.set(word);
            // 把mapper处理后的键值对写到context中
            context.write(k, v);
        }
    }
}

