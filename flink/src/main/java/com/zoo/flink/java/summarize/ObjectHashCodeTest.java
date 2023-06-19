package com.zoo.flink.java.summarize;

import lombok.AllArgsConstructor;

/**
 * @Author: JMD
 * @Date: 6/19/2023
 * 在Java中，默认情况下，Object类的hashCode()方法会返回对象的内存地址的哈希码值，即调用System.identityHashCode(Object x)方法返回的结果。
 * 但是，大多数类都会覆盖hashCode()方法，并根据对象的属性计算哈希码值。

 *  两个数据一致的普通 java 对象的 hashCode 是不同的，所以在 Flink 中，pojo用作 key 时，需要重写 hashCode 和 equal

 *  Flink在内部使用哈希分区策略来将数据流分发到不同的分区中，以提高计算并行性。
 *  * 当使用 POJO 类型作为键（key）时，由于POJO对象中可能包含多个属性，Flink需要使用 POJO 对象的某些属性来计算哈希值，并根据这个哈希值将数据流分配到不同的分区中。

 *  * 因此，在使用POJO类型作为键（key）时，必须覆盖 hashCode()和 equals()方法，以确保相同的键具有相同的哈希值，并且可以进行实际的比较操作。
 */

@AllArgsConstructor
public class ObjectHashCodeTest {
    String name;
    int age;

    public static void main(String[] args) {
        ObjectHashCodeTest people1 = new ObjectHashCodeTest("ming", 12);
        ObjectHashCodeTest people2 = new ObjectHashCodeTest("ming", 12);

        System.out.println(people1.hashCode());
        System.out.println(people2.hashCode());
    }
}
