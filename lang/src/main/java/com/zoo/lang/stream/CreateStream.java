package com.zoo.lang.stream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author: JMD
 * @Date: 6/8/2023
 */
public class CreateStream {

    public static <T> void show(String title, Stream<T> stream){
        final int SIZE = 10;
        List<T> firstElements = stream.limit(SIZE + 1).collect(Collectors.toList());

        System.out.println("size: " + firstElements.size());

        System.out.println(title + ": ");

        for(int i = 0; i < firstElements.size(); i++){
            if(i > 0) System.out.print(", ");

            if(i < SIZE) {
                System.out.print(firstElements.get(i));
            }
            else System.out.println("...");
        }
        System.out.println();
    }
    public static void main(String[] args) throws IOException {
        Path path = Paths.get("input/words.txt");
        String contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

        Stream<String> words = Stream.of(contents.split("\r\n"));
        show("words", words);
    }
}

















