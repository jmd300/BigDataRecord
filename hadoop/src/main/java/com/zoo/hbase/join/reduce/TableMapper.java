package com.zoo.hbase.join.reduce;

import com.zoo.hbase.join.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text, TableBean> {
    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取对应文件名称
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        this.filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        if(filename.contains("order")){
            //封装 outK
            outK.set(split[1]);
            //封装 outV
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setProductName("");
            outV.setFlag("order");
        }else{
            //封装 outK
            outK.set(split[0]);
            //封装 outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setProductName(split[1]);
            outV.setFlag("pd");
        }
        //写出 KV
        // 写入数据报错了：flag字段不能为空也就是这个类的字段不可以为空
        context.write(outK, outV);
    }
}
