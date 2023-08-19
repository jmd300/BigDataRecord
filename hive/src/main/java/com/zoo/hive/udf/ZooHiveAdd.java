package com.zoo.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */

@Description(name = "zoo_hive_add", value = "My UDF add", extended = "SELECT zoo_hive_add(1, 2) FROM table")
public class ZooHiveAdd extends UDF {
    public Integer evaluate(Integer a, Integer b) {
        if (a == null || b == null) {
            return null;
        }
        return a + b;
    }
}
