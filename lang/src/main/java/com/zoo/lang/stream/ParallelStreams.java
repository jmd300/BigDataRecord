package com.zoo.lang.stream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

/**
 * @Author: JMD
 * @Date: 6/14/2023
 */
public class ParallelStreams {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("input/words.txt");
        String contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        List<String> wordsList = Arrays.asList((contents.split("\\PL+")));

        // 会出现多线程问题
        int[] shortWords = new int[10];

        wordsList.parallelStream().forEach(s -> {
            if(s.length() < 10) shortWords[s.length()]++;
        });
        System.out.println("shortWords: " + Arrays.toString(shortWords));

        Map<Integer, Long> shortWordCount = wordsList.parallelStream()
                .filter(s -> s.length() < 10)
                .collect(groupingBy(String::length, counting()));
        System.out.println("shortWordCount: " + shortWordCount);

        // 下游算子不需要排序
        Map<Integer, List<String>> result = wordsList.parallelStream().collect(
                Collectors.groupingByConcurrent(String::length)
        );
        System.out.println("result[6]: " + result.get(6));

        Map<Integer, Long> wordCounts = wordsList.parallelStream().collect(
                groupingByConcurrent(String::length, counting())
        );
        System.out.println("wordCounts: " + wordCounts);
    }
}
