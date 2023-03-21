package com.zoo.flink.wc;

import org.omg.PortableInterceptor.INACTIVE;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @Author: JMD
 * @Date: 3/17/2023
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // WordCount wordCount = new WordCount();

        // wordCount.run(() -> wordCount.getEnv().socketTextStream("hadoop102", 7777));

        String str = "Hello";
        Class<?> cls = str.getClass();
        Field klassField = cls.getDeclaredField("Klass");
        long offset = Unsafe.getUnsafe().objectFieldOffset(klassField);
        Object klassObj = Unsafe.getUnsafe().getObject(str, offset);
        System.out.println(klassObj);

    }
}
