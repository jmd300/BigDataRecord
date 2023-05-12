package com.zoo.flink.java.pojo;

import lombok.AllArgsConstructor;

/**
 * @Author: JMD
 * @Date: 5/11/2023

 * 两个数据一致的普通java对象的hashCode是不同的，所以在Flink中，pojo用作key时，需要重写hashCode和equal

 * Flink在内部使用哈希分区策略来将数据流分发到不同的分区中，以提高计算并行性。当使用POJO类型作为键（key）时，由于POJO对象中可能包含多个属性，
 * Flink需要使用POJO对象的某些属性来计算哈希值，并根据这个哈希值将数据流分配到不同的分区中。

 * 因此，如果POJO类型作为键（key），并且键值用于确定分区，那么就需要重写POJO对象的hashCode()方法，以便确保相同的键具有相同的哈希值。
 * 否则，在哈希桶模数取余的情况下，可能会导致相同键的数据散布在不同的分区中，从而影响计算结果的正确性和性能。

 * 因此，在使用POJO类型作为键（key）时，必须覆盖hashCode()和equals()方法，以确保相同的键具有相同的哈希值，并且可以进行实际的比较操作。
 */
@AllArgsConstructor
public class People {
    String name;
    int age;

    public static void main(String[] args) {
        People people1 = new People("ming", 12);
        People people2 = new People("ming", 12);

        System.out.println(people1.hashCode());
        System.out.println(people2.hashCode());
    }
}
