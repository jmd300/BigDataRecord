package com.zoo.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @Author: JMD
 * @Date: 5/5/2023

 * 在默认情况下，MapReduce 程序的 Reducer 数量是 1。如果没有显式地指定 mapreduce.job.reduces 属性的值或自定义 Partitioner 类，
 * 当 Mapper 结束时，MapReduce 框架会将所有 Mapper 输出的键值对都按照默认的 HashPartitioner 策略进行 Hash 映射，
 * 并将相同 hash 值的键值对发送到同一个 Reducer 进行处理。

 * 修改reducer的数量的方式
 *    1. hadoop jar myjar.jar MyMapReduce -D mapreduce.job.reduces=5 /input /output
 *    2. 通过conf配置，如程序中所示
 *    3. 在代码中编写自定义的 Partitioner 类，覆盖 getPartition 方法，该方法需要返回一个介于 0 到 (numReduceTasks - 1) 范围内的整数，
 *       用于指定该键值对应的 Reducer 编号。这种方式可以手动地将数据划分到相应的 Reducer 中。
 */
class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable intWritable, int numPartitions) {
        // 将相同前缀的单词分到同一个分区中
        String prefix = key.toString().substring(0, 1);
        return prefix.hashCode() % numPartitions;
    }
}
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1、获取job的配置信息
        Configuration conf = new Configuration();

        // 设置reducer的数量
        conf.set("mapreduce.job.reduces", "2");

        Job job = Job.getInstance(conf);

        // 2、设置jar的加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3、分别设置Mapper和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入输出路径
        /*6、设置输入输出路径*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // FileInputFormat.setInputPaths(job, new Path("input/words.txt"));
        // FileOutputFormat.setOutputPath(job, new Path("output/WordCountOut"));

        // 7、提交任务
        boolean flag = job.waitForCompletion(true);
        System.out.println("flag : " + flag);
    }
}
