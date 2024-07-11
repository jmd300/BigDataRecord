package com.zoo.lang.concurrency;

import java.util.concurrent.Semaphore;

public class SemaphoreExample {
    public static void main(String[] args) {
        Semaphore semaphore = new Semaphore(10);

        int queueLength = semaphore.getQueueLength();

    }
}
