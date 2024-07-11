package com.zoo.lang.concurrency.DoubleCheckLockExample;

/**
 * synchronized到底能不能禁止指令重排序？不能
 * https://blog.csdn.net/wx17343624830/article/details/132501682
 */
public class MySingleton {
    private static MySingleton INSTANCE;
    private MySingleton() {}
    public static MySingleton getInstance() {
        if (INSTANCE == null) {
            synchronized (MySingleton.class) {
                if (INSTANCE == null) {
                    // 可能会造成指令重排序的情况，
                    INSTANCE = new MySingleton();
                }
            }
        }
        return INSTANCE;
    }

    public static void main(String[] args) {
        System.out.println("Singleton");
    }
}
