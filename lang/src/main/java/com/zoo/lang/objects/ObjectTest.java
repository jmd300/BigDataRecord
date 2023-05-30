package com.zoo.lang.objects;

import org.junit.Test;

import java.util.Objects;

/**
 * @Author: JMD
 * @Date: 5/30/2023
 */
public class ObjectTest {
    @Test
    public void testNull() {
        String s = null;

        // 判断是否为null, true
        System.out.println(Objects.isNull(s));

        // 判断是否不为null, false
        System.out.println(Objects.nonNull(s));
    }

}
