package com.zoo.hbase.join.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 1 获取 job 信息
        Configuration conf = new Configuration();
        // 开启 map 端输出压缩

        // conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置 map 端输出压缩方式
        // conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);

        // 2 设置加载 jar 包路径
        job.setJarByClass(MapJoinDriver.class);

        // 3 关联 mapper
        job.setMapperClass(MapJoinMapper.class);

        // 4 设置 Map 输出 KV 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 加载缓存数据
        job.addCacheFile(new URI("file:///Y:/input/cached/pd.txt"));

        // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0
        job.setNumReduceTasks(0);


        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("Y:\\input\\order"));
        FileOutputFormat.setOutputPath(job, new Path("Y:\\mapJoinOutput"));

        // 设置 reduce 端输出压缩开启
        // FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式，bzip2可切片
        // FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        // 7 提交
        boolean ret  = job.waitForCompletion(true);
        System.out.println("ret is: " + ret);
        System.exit(ret ? 0 : 1);
    }
}
