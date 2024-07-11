package com.zoo.lang;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class ListExample {
    @Test
    public void testList1() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ArrayList<Integer> list =new ArrayList<Integer>();
        Method method = list.getClass().getMethod("add", Object.class);
        method.invoke(list, "Java反射机制实例");
        System.out.println(list.get(0));
    }
    public static void main(String[] args) {

    }
}
