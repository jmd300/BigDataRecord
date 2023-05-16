package com.zoo.flink.java.sink;

import com.zoo.flink.java.FlinkEnv;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * @Author: JMD
 * @Date: 5/12/2023
 */
public class SinkCustomToHBase extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        env.fromElements("hello", "world")
                .addSink(
                        new RichSinkFunction<String>() {
                            // 管理 Hbase 的配置信息,这里因为 Configuration 的重名问题，将类以完整路径导入
                            public org.apache.hadoop.conf.Configuration configuration;
                            // 管理 Hbase 连接
                            public Connection connection;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                configuration = HBaseConfiguration.create();
                                configuration.set("hbase.zookeeper.quorum", "hadoop102:2181");
                                connection = ConnectionFactory.createConnection(configuration);
                            }
                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                // 表名为 test
                                Table table = connection.getTable(TableName.valueOf("test"));
                                // 指定 row key写入的数据
                                Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8));
                                // 指定列名写入的数据
                                put.addColumn("info".getBytes(StandardCharsets.UTF_8) , value.getBytes(StandardCharsets.UTF_8),
                                        "1".getBytes(StandardCharsets.UTF_8));

                                table.put(put); // 执行 put 操作
                                table.close(); // 将表关闭
                            }
                            @Override
                            public void close() throws Exception {
                                super.close();
                                connection.close(); // 关闭连接
                            }
                        }
                );
        env.execute();
    }
}
