package com.zoo.flink.java.operator;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class TransFilterDemo extends FlinkEnv {
    public static class UserFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event e) throws Exception {
            return e.user.equals("Mary");
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 传入匿名类实现 FilterFunction
        arrayStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event e) throws Exception {
                return e.user.equals("Mary");
            }
        });

        // 2. 传入Function实现类
        arrayStream.filter(new UserFilter());

        // 3. 传入lambda函数
        arrayStream.filter(e -> e.user.equals("Mary"));

        env.execute();
    }
}