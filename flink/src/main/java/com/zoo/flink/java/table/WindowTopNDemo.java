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
 * @Date: 6/19/2023

 * 表聚合函数
 * 表聚合函数是指对表中的一组行进行聚合操作的函数。
 * 在 Flink 中，Table API 或 SQL 提供的内置聚合函数（如 SUM、COUNT、AVG、MAX、MIN 等），或自行编写自定义聚合函数。
 * 使用表聚合函数时可以使用 GROUP BY 和 HAVING 子句，以根据指定字段对数据进行分组和筛选。

 * 窗口表值函数
 * 窗口表值函数是指在指定的时间窗口期间计算每个时间窗口的结果并将其输出到表中的函数。
 * 在 Flink 中，可以使用 Table API 或 SQL 提供的内置窗口函数（如 TUMBLE、HOP、SESSION 等）来定义窗口，也可以自行编写自定义窗口函数。
 * 使用窗口表值函数时需要指定窗口大小和滑动步长，并设置适当的窗口操作，例如聚合操作或窗口连接操作。

 * 总结：
 * 表聚合函数和窗口表值函数是 Flink 中处理流数据的两个重要概念。
 * 表聚合函数用于对一组行进行聚合操作，可使用 GROUP BY 和 HAVING 子句对数据进行分组和筛选；窗口表值函数用于计算指定时间窗口期间的结果，并将其输出到表中。
 */
public class WindowTopNDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> eventStream = arrayStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
        );

        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                // 将 timestamp 指定为事件时间，并命名为 ts
                $("timestamp").rowtime().as("ts")
        );

        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery = "SELECT window_start, window_end, user, COUNT(url) as cnt " +
                "FROM TABLE ( " +
                "TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
                "GROUP BY window_start, window_end, user ";

        // 定义 Top N 的外层查询
        String topNQuery =
                "SELECT * " +
                        "FROM (" +
                        "SELECT *, " +
                        "ROW_NUMBER() OVER ( " +
                        "PARTITION BY window_start, window_end " +
                        "ORDER BY cnt desc " +
                        ") AS row_num " +
                        "FROM (" + subQuery + ")) " +
                        "WHERE row_num <= 2";

        Table table = tableEnv.sqlQuery(topNQuery);
        table.printSchema();
        tableEnv.toDataStream(table).print();
        env.execute();
    }
}
