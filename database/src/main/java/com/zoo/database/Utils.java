package com.zoo.database;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: JMD
 * @Date: 5/17/2023
 */
public class Utils {
    public static void printListArray(List<Object[] > listArr){
        if(listArr == null){
            System.out.println("null");
            return;
        }
        listArr.forEach(arr -> {
            String row = Arrays.stream(arr)
                    .map(o -> {
                        if (o == null) return "null";
                        else return o.toString();
                    })
                    .map(String::valueOf)
                    .collect(Collectors.joining(" "));

            System.out.println(row);
        });
    }
}
