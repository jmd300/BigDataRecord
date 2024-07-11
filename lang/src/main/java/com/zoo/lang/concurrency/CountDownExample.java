package com.zoo.lang.concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

public class CountDownExample {
    int i= 0;
    synchronized void synchronizedMethod(){
        i++;
        System.out.println("同步方法");
    }

    private final ReentrantLock lock = new ReentrantLock();
    void lockMethod(){
        lock.lock();
        try {
            i++;
        }finally {
            lock.unlock();
        }
    }

    static CountDownLatch countDownLatch = new CountDownLatch(2);
    static void run(){
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("小明吃完饭了");
            countDownLatch.countDown();
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("小雨吃完饭了");
            countDownLatch.countDown();
        }).start();
    }

    public static void main(String[] args) throws InterruptedException {
        run();
        countDownLatch.await();
        System.out.println("小孩吃完饭去玩了！");
    }
}
