package com.zoo.lang;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

public class Example {

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    HashMap<String, String> map = new HashMap();
    @Test
    public void testIntern(){
        String s = "a";

        // new String 时a进入到字符串常量池中
        String s1 = new String("a");
        // 字符串常量池中存在了a，所以返回原来的引用
        String sCopy = s1.intern();
        System.out.println(s == sCopy);
        System.out.println("a" == sCopy);

        System.out.println(s1 == sCopy);

        String s2 = "a";
        System.out.println(s1 == s2);

        String s3 = new String("a") + new String("a");
        // 将s3的引用放入到字符串常量池中
        s3.intern();
        // 使用字符串常量池中的数据
        String s4 = "aa";
        // 所以相等
        System.out.println(s3 == s4);

        Optional<String> stringOptional = Optional.empty();
        System.out.println(stringOptional.get());
    }

    @Test
    public void threadLocal(){
        ThreadLocal<String> threadLocal = new ThreadLocal<>();
        threadLocal.set("haha");
        threadLocal.get();
    }
}
