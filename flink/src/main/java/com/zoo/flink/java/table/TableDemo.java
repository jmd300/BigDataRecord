package com.zoo.flink.java.table;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author: JMD
 * @Date: 6/16/2023

 * 创建一个“表环境”（ TableEnvironment），然后将数据流（DataStream）转换成一个表（Table）；
 * 之后就可以执行 SQL 在这个表中查询数据了。
 * 查询得到的结果依然是一个表，把它重新转换成流就可以打印输出了。
 */
public class TableDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(arrayStream);

        // 获取表环境的配置
        TableConfig tableConfig = tableEnv.getConfig();
        // 配置状态保持时间
        tableConfig.setIdleStateRetention(Duration.ofMinutes(60));

        System.out.println(eventTable.getResolvedSchema());

        System.out.println("eventTable: " + eventTable);

        // 用执行 SQL 的方式提取数据
        Table visitTable = tableEnv.sqlQuery("select url, user from " + eventTable);

        // 转换为临时表的示例
        // tableEnv.createTemporaryView("MyTable", eventDataStreamSource);
        // tableEnv.createTemporaryView("MyTable", visitTable);

        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable).print();

        // 执行程序
        env.execute();
    }
}
