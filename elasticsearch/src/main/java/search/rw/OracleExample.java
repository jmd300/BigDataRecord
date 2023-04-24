package search.rw;

import search.OracleConf;

import java.sql.*;

/**
 * @Author: JMD
 * @Date: 3/14/2023
 */

public class OracleExample {
    public static void main(String[] args) {
        try {
            // 建立数据库连接
            Connection conn = OracleConf.getConnection();
            // 执行查询
            PreparedStatement ps = conn.prepareStatement("select AUTHOR, topic, VERSE  from poetry");
            ResultSet rs = ps.executeQuery();

            // 处理查询结果
            while (rs.next()) {
                System.out.println("author: " + rs.getRowId(1));
                System.out.println("title: " + rs.getRowId(2));
                System.out.println("text: " + rs.getRowId(3));
            }

            // 关闭查询结果和语句对象
            rs.close();
            ps.close();

            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
/*
            // 执行更新
            ps = conn.prepareStatement("UPDATE employees SET department = ? WHERE id = ?");
            ps.setString(1, "marketing");
            ps.setInt(2, 1001);
            int updateCount = ps.executeUpdate();

            // 输出更新记录数
            System.out.println("Updated " + updateCount + " records");

            // 关闭更新语句对象和数据库连接
            ps.close();
            */