package com.zoo.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class UdfDemo extends UDF {
    public Integer evaluate(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
