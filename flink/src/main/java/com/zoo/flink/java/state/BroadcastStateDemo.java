package com.zoo.flink.java.state;

import com.zoo.flink.java.util.FlinkEnv;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: JMD
 * @Date: 6/14/2023
 */
public class BroadcastStateDemo extends FlinkEnv {
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Action {
        public String userId;
        public String action;

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }
    // 定义行为模式 POJO 类，包含先后发生的两个行为
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Pattern {
        public String action1;
        public String action2;

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy"));

        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env.fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy"));

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        // 将事件流和广播流连接起来，进行处理
        DataStream<Tuple2<String, Pattern>> matches = actionStream
                .keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternEvaluator());
        matches.print();
        env.execute();
    }
    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;
        @Override
        public void open(Configuration conf) {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAction", Types.STRING));
        }

        /**
         * 这里我们将检测的行为模式定义为 POJO 类 Pattern，里面包含了连续的两个行为。
         * 由于广播状态中只保存了一个 Pattern，并不关心 MapState 中的 key，所以也可以直接将 key 的类型指定为 Void，具体值就是 null。
         * 在具体的操作过程中，我们将广播流中的 Pattern 数据保存为广播变量；
         * 在行为数据 Action 到来之后读取当前广播变量，确定行为模式，并将之前的一次行为保存为一个 ValueState——这是针对当前用户的状态保存，所以用到了 Keyed State。
         * 检测到如果前一次行为与 Pattern 中的 action1 相同，而当前行为与 action2 相同，则发现了匹配模式的一组行为，输出检测结果
         */
        @Override
        public void processBroadcastElement(Pattern pattern, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))
            );
            // 将广播状态更新为当前的 pattern
            bcState.put(null, pattern);
        }
        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            Pattern pattern = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(action.action);
        }
    }
}
