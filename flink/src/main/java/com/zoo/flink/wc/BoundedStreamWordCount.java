package com.zoo.flink.wc;

/**
 * @Author: JMD
 * @Date: 3/17/2023
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        WordCount wordCount = new WordCount();

        wordCount.run(() -> wordCount.getEnv().readTextFile("input/words.txt"));
    }
}
