package com.zoo.lang.stream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @Author: JMD
 * @Date: 6/13/2023
 */
public class UseStream {
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("input/words.txt");
        String contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        Stream<String> words1 = Stream.of(contents.split("\r\n"));

        Optional<String> largest = words1.max(String::compareToIgnoreCase);
        System.out.println("largest: " + largest.orElse(""));

        Stream<String> words2 = Stream.of(contents.split("\r\n"));
        Optional<String> startsWithH = words2.filter(s -> s.startsWith("h")).findFirst();
        System.out.println("startsWithH: " + startsWithH);

        Stream<String> words3 = Stream.of(contents.split("\r\n"));
        Optional<String> startsWithF = words3.filter(s -> s.startsWith("f")).findAny();
        System.out.println("startsWithF: " + startsWithF);
    }
}
