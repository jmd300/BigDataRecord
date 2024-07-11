package com.zoo.hbase.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

import com.zoo.hbase.join.TableBean;

public class TableDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // 设置reducer的数量
        conf.set("mapreduce.job.reduces", "2");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        // job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("Y:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("output/tableOut"));

        boolean ret = job.waitForCompletion(true);

        System.out.println("ret is: " + ret);
        System.exit(ret ? 0 : 1);
    }
}
