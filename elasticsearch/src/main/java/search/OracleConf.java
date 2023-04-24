package search;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: JMD
 * @Date: 3/14/2023
 */
public class OracleConf {
    static String jdbcUrl = "jdbc:oracle:thin:@//localhost:1521/XE";
    static String user = "C##JMD";
    static String password = "888666";

    // 加载Oracle JDBC驱动
    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    static public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }
}
