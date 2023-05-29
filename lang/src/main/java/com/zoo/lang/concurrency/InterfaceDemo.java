package com.zoo.lang.concurrency;

/**
 * @Author: JMD
 * @Date: 3/13/2023
 */
public interface InterfaceDemo {
    /**
     * java8中，接口是可以有实现了的默认的方法的。
     * @return 2
     */
    default int add(){
        return 2;
    };

    // 接口是不可以被new的
    public static void main(String[] args) {
        // new InterfaceDemo()
    }
}
