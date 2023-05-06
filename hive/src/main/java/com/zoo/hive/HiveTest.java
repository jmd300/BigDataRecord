package com.zoo.hive;
/**
 * @Author: JMD
 * @Date: 5/5/2023
 * 参考博客：
 * Hiveserver2 Beeline连接设置用户名和密码：https://blog.csdn.net/ZTwufeng/article/details/119701387
 * Hive网页打不开：https://blog.csdn.net/shijinxin3907837/article/details/103150910
 * Hive示例程序：https://cloud.tencent.com/document/product/589/19021
 */
import java.sql.*;

/**
 * 运行确实很慢
 */
public class HiveTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("begin connection");
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://hadoop102:10000/default", "root", "admin");

        System.out.println("after connection");
        Statement stmt = con.createStatement();
        String tableName = "HiveTestByJava";

        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        System.out.println("Create table success!");

        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        sql = "insert into " + tableName + " values (42,\"hello\"),(48,\"world\")";
        stmt.execute(sql);

        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);

        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t"
                    + res.getString(2));
        }

        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}