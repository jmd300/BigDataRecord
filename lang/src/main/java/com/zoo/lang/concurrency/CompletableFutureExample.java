package com.zoo.lang.concurrency;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 多线程计算结果，然后结果汇集
 */
public class CompletableFutureExample{
    public static void main(String[] args) {
        List<Integer> nums = Arrays.asList(5, 8, 6, 7, 9, 10);
        List<CompletableFuture<Integer>> futures = nums.stream()
                .map(value -> CompletableFuture.supplyAsync(() -> {
                    return value * 2;
                }))
                .collect(Collectors.toList());


        CompletableFuture[] array = futures.toArray(new CompletableFuture[0]);

        // 将CompletableFuture对象数组转换为CompletableFuture对象数组
        CompletableFuture<Integer> sumFuture = CompletableFuture.allOf(array)
                .thenApplyAsync(v -> {
                   int sum = futures.stream().mapToInt(CompletableFuture::join).sum();
                   return sum;
                });
        Integer ret = sumFuture.join();
        System.out.println("ret is: " + ret);

        // CompletableFuture 源码阅读

    }
}