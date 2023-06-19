package com.zoo.flink.java.util;

import lombok.AllArgsConstructor;

import java.sql.Timestamp;

/**
 * @Author: JMD
 * @Date: 5/15/2023
 */
@AllArgsConstructor
public class UrlViewCount {
    public String url;
    public Long count;

    public Long windowStart;
    public Long windowEnd;
    public UrlViewCount() {
    }
    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
