package com.zoo.flink.java.cep;

import com.zoo.flink.java.util.Event;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @Author: JMD
 * @Date: 7/19/2023
 * 继承 TimedOutPartialMatchHandler 的类语法上感觉不一定需要继承 PatternProcessFunction，
 * 但是正藏情况下需要处理匹配到的事件，所以自定的 PatternProcessFunction 需要 继承 PatternProcessFunction
 */

class MyPatternProcessFunction extends PatternProcessFunction<Event, String> implements TimedOutPartialMatchHandler<Event> {

    // 超时部分匹配事件的处理
    // 定义输出标签（OutputTag）。调用 ctx.output()方法，就可以将超时的匹配事件输出到侧输出流了。
    @Override
    public void processTimedOutMatch(Map<String, List<Event>> match, PatternProcessFunction.Context ctx) throws Exception {
        Event startEvent = match.get("start").get(0);
        OutputTag<Event> outputTag = new OutputTag<Event>("time-out") {};
        ctx.output(outputTag, startEvent);
    }

    // 正常匹配事件的处理
    @Override
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {

    }
}