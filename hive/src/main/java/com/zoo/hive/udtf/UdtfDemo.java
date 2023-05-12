package com.zoo.hive.udtf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 * UDTF是User-Defined Table-Generating Functions 的缩写，即用户定义的表生成函数。UDTF 用于从原始表中的一行生成多行数据。
 * 典型的 UDTF有EXPLODE、posexplode等函数，它能将array或者map展开。

 * 表生成函数和聚合函数是相反的，表生成函数可以把单列扩展到多列。表生成函数：可以理解为一个函数可以生成一个表。
 */
public class UdtfDemo extends GenericUDTF {
    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args.length != 2) {
            return;
        }
        String input = args[0].toString();
        String separator = args[1].toString();
        String[] splits = input.split(separator);
        for (String split : splits) {
            forward(split);
        }
    }
    @Override
    public void close() throws HiveException {
    }
}