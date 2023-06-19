package com.zoo.flink.java.table;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: JMD
 * @Date: 6/19/2023
 */
public class TopNDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = arrayStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
        );
        tableEnv.createTemporaryView("EventTable", stream);

        // timestamp 是不可以直接使用的，需要加反单引号，注意sql的每行之间留空格或者换行
        String sql = "SELECT user, url, `timestamp` as ts, row_num" +
                " FROM (" +
                " SELECT *," +
                " ROW_NUMBER() OVER (" +
                " PARTITION BY user" +
                " ORDER BY CHAR_LENGTH(url) desc" +
                " ) AS row_num" +
                " FROM EventTable)" +
                " WHERE row_num <= 2";
        Table table = tableEnv.sqlQuery(sql);
        table.printSchema();
        tableEnv.toChangelogStream(table).print();
        env.execute();
    }
}
