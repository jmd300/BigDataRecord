package com.zoo.flink.java.cep;

import com.zoo.flink.java.util.FlinkEnv;

import com.zoo.flink.java.util.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
/**
 * @Author: JMD
 * @Date: 6/20/2023
 */
public class OrderTimeoutDetect extends FlinkEnv {
    static OutputTag<String> timeoutTag = new OutputTag<String>("timeout", Types.STRING);

    public static void main(String[] args) throws Exception {
        // 获取订单事件流，并提取时间戳、生成水位线
        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (event, l) -> event.timestamp)
                )
                .keyBy(order -> order.orderId); // 按照订单 ID 分组

        Pattern<OrderEvent, ?> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                }).followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                }).within(Time.minutes(15)); // 限制在 15 分钟之内

        // 2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);

        // 3. 将匹配到的，和超时部分匹配的复杂事件提取出来，然后包装成提示信息输出
        SingleOutputStreamOperator<String> payedOrderStream = patternStream.process(new OrderPayPatternProcessFunction());

        // 将正常匹配和超时部分匹配的处理结果流打印输出
        payedOrderStream.print("payed");

        payedOrderStream.getSideOutput(timeoutTag).print("timeout");
        env.execute();
    }

    // 实现自定义的 PatternProcessFunction，需实现 TimedOutPartialMatchHandler 接口
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String>
            implements TimedOutPartialMatchHandler<OrderEvent> {
        // 处理正常匹配事件
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent payEvent = match.get("pay").get(0);
            out.collect("订单 " + payEvent.orderId + " 已支付！ ");
        }
        // 处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent createEvent = match.get("create").get(0);
            ctx.output(timeoutTag, "订单 " + createEvent.orderId + " 超时未支付！用户为： " + createEvent.userId);
        }
    }
}
