package com.zoo.lang.concurrency;

import java.util.ArrayList;

/**
 * @Author: JMD
 * @Date: 3/16/2023
 */
class SumThread<T extends Number> extends Thread {
    ArrayList<T> arr;
    SumThread(ArrayList<T> arr) {
        this.arr = arr;
    }

    public Double sum() {
        return arr.stream().mapToDouble(Number::doubleValue).sum();
    }
    public void run(){
        System.out.println(sum());
    }
}
