package com.zoo.lang.concurrency;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.val;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorExample {
    public static void main(String[] args) {
        val pool1 = new ThreadPoolExecutor(10, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));

        val nameThreadFactory = new ThreadFactoryBuilder().setNameFormat("guava-demo-pool-%d").build();

        val pool2 = new ThreadPoolExecutor(5, 200, 0L, TimeUnit.MICROSECONDS, new LinkedBlockingDeque<>(1024), nameThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        pool1.execute(new Thread(() -> {
            System.out.println("hello executor");
        }));
    }
}
