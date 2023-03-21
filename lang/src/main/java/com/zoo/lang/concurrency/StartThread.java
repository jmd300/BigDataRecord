package com.zoo.lang.concurrency;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 3/13/2023
 */
public class StartThread {
    public static void main(String[] args) throws InterruptedException {
        ArrayList<Integer> arr1 = new ArrayList<Integer>();

        arr1.add(1);
        arr1.add(2);
        arr1.add(3);
        Thread t1 = new SumThread<Integer>(arr1);
        t1.start();

        while (t1.isAlive()){
            System.out.println("main thread do something else...");
            TimeUnit.MILLISECONDS.sleep(100);
        }
    }
}
