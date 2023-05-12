package com.zoo.flink.java.sink;

import com.zoo.flink.java.FlinkEnv;
import com.zoo.flink.java.pojo.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: JMD
 * @Date: 5/11/2023
 */
public class SinkToMysqlDemo extends FlinkEnv {
    // 对于 MySQL 5.7， 用"com.mysql.jdbc.Driver"
    public static void main(String[] args) throws Exception {
        arrayStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO clicks (user, url) VALUES (?, ?)",
                        (JdbcStatementBuilder<Event>) (ps, event) -> {
                            ps.setString(1, event.user);
                            ps.setString(2, event.url);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://hadoop102:3306/data")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("1234")
                                .build()
                ));
        env.execute();
    }
}
