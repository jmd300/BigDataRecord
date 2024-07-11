package com.zoo.lang.guava;

// import com.google.common.base.Objects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.List;

public class GuavaExample {

    public static void main(String[] args) {
        //1. 使用Optional代替null等语义不清晰，可能出现null的情况
        // Guava 包里的Optional
        Optional<String> optional1 = Optional.absent();
        if(optional1.isPresent()){
            System.out.println(optional1.get());
        }

        // 2. 前置条件判断
        Preconditions.checkArgument(args.length < 10, "args length must lg 10");

        Preconditions.checkNotNull(args);
        Preconditions.checkElementIndex(5, 6);
        //Preconditions.checkPositionIndexes(int start, int end, int size)

        // 3. 常见的Objects方法
        Integer a = 10;
        Integer b = 10;
        System.out.println(a == null);
        System.out.println(Objects.equal(null, a));
        System.out.println(Objects.equal(a, b));
        System.out.println(Objects.equal(a, 10));

        // 4.排序 Ordering
        // 从实现上说，Ordering 实例就是一个特殊的 Comparator 实例。Ordering 把很多基于 Comparator 的静态方法（如 Collections.max）包装为自己的实例方法（非静态方法），并且提供了链式调用方法，来定制和增强现有的比较器。
        Ordering<String> byLengthOrdering = new Ordering<String>() {
            public int compare(String left, String right) {
                return Integer.compare(left.length(), right.length());
            }
        };

        // reverse()	获取语义相反的排序器
        // nullsFirst()	使用当前排序器，但额外把 null 值排到最前面。
        // nullsLast()	使用当前排序器，但额外把 null 值排到最后面。
        // compound(Comparator)	合成另一个比较器，以处理当前排序器中的相等情况。
        // lexicographical()	基于处理类型 T 的排序器，返回该类型的可迭代对象 Iterable<T>的排序器。
        // onResultOf(Function)	对集合中元素调用 Function，再按返回值用当前排序器排序。

        // 5. 不可变集合
        ImmutableSet immutableSet = ImmutableSet.of("hello", "scala", "java", "hello");
        immutableSet.forEach(System.out::println);
        final List<String> list = Lists.newArrayList("java", "final");
        // final修饰的对象中的属性/数据是可以修改的
        System.out.println("final的数组其中的内容是否可以修改，还是说只有引用不可改");
        list.set(0, "scala");
        System.out.println(list.get(0));

        ImmutableSet<String> immutableSet1 = ImmutableSet.of("a", "b");

    }
    public int compareTo(GuavaExample that) {
        return ComparisonChain.start()
                .compare(this.name, that.name)
                .compare(this.age, that.age)
//                 .compare(this.anEnum, that.anEnum, Ordering.natural().nullsLast())
                .result();
    }




    String name;
    Integer age;
}
