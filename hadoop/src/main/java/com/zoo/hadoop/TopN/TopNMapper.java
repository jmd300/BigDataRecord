package com.zoo.hadoop.TopN;

import lombok.Setter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
public class TopNMapper extends Mapper<LongWritable, Text, People, Text> {
    @Setter
    int topN = 10;

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private final TreeMap<People, Text> treeMap = new TreeMap<People, Text>();
    private People people;
    private Text text;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (People people : treeMap.keySet()) {
            context.write(people, treeMap.get(people));
        }
    }

    /**
     *
     * @param key      输入数据中的Key，对于文本文件来说就是行号
     * @param value    输入数据中的value，对于文本文件来说就是行字符串
     * @param context  执行环境
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        people = new People();
        text = new Text();

        String[] fields =value.toString().split(",");
        people.setId(fields[0]);
        people.setName(fields[1]);
        people.setWeight(Double.parseDouble(fields[2]));

        text.set(fields[0]);
        treeMap.put(people, text);

        if(treeMap.size() > topN){
            treeMap.remove(treeMap.firstKey());
        }
    }
}
