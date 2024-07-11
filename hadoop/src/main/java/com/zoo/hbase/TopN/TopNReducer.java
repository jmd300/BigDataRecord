package com.zoo.hbase.TopN;

import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
public class TopNReducer extends Reducer<People, Text, Text, People> {
    @Setter
    int topN = 10;

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private final TreeMap<People, Text> treeMap = new TreeMap<People, Text>();

    @Override
    protected void reduce(People key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            People bean = new People(key.getId(), key.getName(), key.getWeight());
            treeMap.put(bean, new Text(value));

            if (treeMap.size() > topN) {
                treeMap.remove(treeMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (People people : treeMap.keySet()) {
            context.write(new Text(treeMap.get(people)), people);
        }
    }
}
