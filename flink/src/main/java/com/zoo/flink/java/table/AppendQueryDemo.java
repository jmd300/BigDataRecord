package com.zoo.flink.java.table;

import com.zoo.flink.java.util.Event;
import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: JMD
 * @Date: 6/16/2023
 *
 * sql查询，输出中仅 insert 数据，不修改或者撤回
 */
public class AppendQueryDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = arrayStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                );
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                // 将 timestamp 指定为事件时间，并命名为 ts
                $("timestamp").rowtime().as("ts")
        );

        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 设置 1 小时滚动窗口，执行 SQL 统计查询
        Table result = tableEnv.sqlQuery(
                        "SELECT user, " +
                                "window_end AS endT, " + // 窗口结束时间
                                "COUNT(url) AS cnt " + // 统计 url 访问次数
                                "FROM TABLE( " +
                                    "TUMBLE(TABLE EventTable, " + // 1 小时滚动窗口
                                    "DESCRIPTOR(ts), " +
                                    "INTERVAL '1' HOUR)" +
                                ") " +
                                "GROUP BY user, window_start, window_end "
                );
        tableEnv.toDataStream(result).print();
        env.execute();
    }
}
