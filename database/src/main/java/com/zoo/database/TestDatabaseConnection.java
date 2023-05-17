package com.zoo.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Author: JMD
 * Date: 5/16/2023
 */
public class TestDatabaseConnection {
    public static void main(String[] args) {
        String db       = args[0];
        String ip       = args[1];
        String port     = args[2];
        String database = args[3];
        String username = args[4];
        String password = args[5];

        String url  = null;
        String driverCLassName = null;

        if(db.equals("mysql")){
            url = "jdbc:mysql://" + ip + ":" + port +"/" + database + "?useServerPrepStmts=true";
            driverCLassName = "com.mysql.jdbc.Driver";

        }else if(db.equals("sql-server")){
            url = "jdbc:sqlserver://" + ip + ":" + port + ";databaseName=" + database + ";encrypt=true;trustServerCertificate=true";
            driverCLassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        try {
            Class.forName(driverCLassName);
            assert url != null;
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println(connection);
        } catch (SQLException e) {
            System.out.println("SQLException");
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            System.out.println("ClassNotFoundException");
        }
    }
}