package com.zoo.lang.stream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @Author: JMD
 * @Date: 6/13/2023
 */
public class OptionalDemo {
    public static void main(String[] args) throws IOException {
        String contents = new String(Files.readAllBytes(Paths.get("input/words.txt")), StandardCharsets.UTF_8);

        List<String> wordList = Arrays.asList(contents.split("\\PL+"));

        Optional<String> optionalValue = wordList.stream()
                .filter(s -> s.contains("sp"))
                .findFirst();
        System.out.println(optionalValue.orElse("No word") + " contains sp");

        Optional<String> optionalString = Optional.empty();
        String result = optionalString.orElse("N/A");
        System.out.println("result: " + result);

        result = optionalString.orElseGet(() -> Locale.getDefault().getDisplayName());
        System.out.println("result: " + result);
        try{
            result = optionalString.orElseThrow(IllegalStateException::new);
            System.out.println("result: " + result);
        }catch (Throwable t){
            t.printStackTrace();
        }
        System.out.println("catch");

        wordList = Arrays.asList(contents.split("\\PL+"));
        optionalValue = wordList.stream()
                .filter(s -> s.contains("flink"))
                .findFirst();

        optionalValue.ifPresent(s -> System.out.println(s + " contains flink"));

        HashSet<String> results = new HashSet<String>();
        optionalValue.ifPresent(results::add);
        Optional<Boolean> added = optionalValue.map(results::add);
        System.out.println(added);

        System.out.println("1: " + inverse(4.0).flatMap(OptionalDemo::squareRoot));
        System.out.println("2: " + inverse(-4.0).flatMap(OptionalDemo::squareRoot));
        System.out.println("3: " + inverse(0.0).flatMap(OptionalDemo::squareRoot));

        Optional<Double> result2 = Optional.of(-4.0).flatMap(OptionalDemo::inverse).flatMap(OptionalDemo::squareRoot);
        System.out.println("result2: " + result2);

        /**
         * map 方法将 Optional 中包含的对象 t 映射成一个对象 u，并将其封装在 Optional 中返回。如果 map 映射结果为 null，则会返回一个空的 Optional。
         * flatMap 方法则是将 Optional 中包含的对象 t 转换成另外一个 Optional 对象 u，如果 u 本身已经是一个 Optional 对象，则直接返回 u。与 map
         * 不同的是，flatMap 的结果不能为 null，否则会抛出 NullPointerException异常。
         * 需要注意的是，flatMap 的映射函数需要返回一个 Optional 对象，这样 flatMap 才能自动将其中的 Optional 展开并返回其内部封装的对象。
         * 而对于 map 方法来说，则只需要保证映射函数可以将类型 T 转换成类型 U 即可。
         */
        // 使用 map 方法将 Optional<String> 转换成 Optional<Integer>
        Optional<String> string1 = Optional.of("123");
        Optional<Integer> integer1 = string1.map(Integer::valueOf);

        // 使用 flatMap 方法将 Optional<String> 转换成 Optional<Integer>
        Optional<String> string2 = Optional.of("123");
        Optional<Integer> integer2 = string2.flatMap(s -> Optional.of(Integer.valueOf(s)));
    }
    public static Optional<Double> inverse(Double x){
        return x == 0 ? Optional.empty() : Optional.of(1 / x);
    }

    public static Optional<Double> squareRoot(Double x){
        return x < 0 ? Optional.empty() : Optional.of(Math.sqrt(x));
    }
}
