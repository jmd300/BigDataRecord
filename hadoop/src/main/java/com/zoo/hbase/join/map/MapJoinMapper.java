package com.zoo.hbase.join.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> productMap = new HashMap<>();
    private Text text = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //通过缓存文件得到小表数据 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行
            //01 小米
            String[] split = line.split("\t");
            productMap.put(split[0], split[1]);
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("in map");
        // 1001 01 1
        String[] fields = value.toString().split("\t");
        System.out.println("line is: " + value.toString());
        System.out.println("1");
        //通过大表每行数据的 pid,去 pdMap 里面取出 productName
        String productName = productMap.get(fields[1]);
        System.out.println("2");
        //将大表每行数据的 pid 替换为 productName
        System.out.println("fields[0] is: " + fields[0]);
        System.out.println("fields[1] is: " + fields[1]);
        System.out.println("fields[2] is: " + fields[2]);
        System.out.println("productName is: " + productName);
        text.set(fields[0] + "\t" + productName + "\t" + fields[2]);
        System.out.println("3");
        //写出
        context.write(text, NullWritable.get());
        System.out.println("over map");
    }
}
