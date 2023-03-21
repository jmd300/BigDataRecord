package com.zoo.lang.concurrency;

import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 3/13/2023
 */
public class ThreadIntroduce {
    public static void main(String[] args) {
        //打印当前线程组的线程
        Thread.currentThread().getThreadGroup().list();

        System.out.println("=========");
        //idea用的是反射,还有一个monitor监控线程，使用java concurrency.ThreadIntroduce 运行恒旭时只有一个线程
        System.out.println(Thread.activeCount());

        Runtime.getRuntime().exit(0);

        // 程序睡眠
        try {
            // 微秒
            TimeUnit.MICROSECONDS.sleep(10);
            // 毫秒
            TimeUnit.MILLISECONDS.sleep(10);
            // s
            TimeUnit.SECONDS.sleep(10);
            // 分钟
            TimeUnit.MINUTES.sleep(10);
            // 小时
            TimeUnit.HOURS.sleep(1);
            // 天
            TimeUnit.DAYS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
