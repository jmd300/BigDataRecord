package com.zoo.database;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author GUOWE
 */
public class IMysql {
    Connection con = null;
    static String path = "file:Y:\\jars\\mysql-connector-java-5.1.49.jar";
    static URLClassLoader urlClassLoader;

    static {
        try {
            urlClassLoader = new URLClassLoader(new URL[]{new URL(path)});
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public IMysql(DataBaseConfig config) {
        this.con = getConnection(config);
    }

    static private Connection getConnection(DataBaseConfig config){
        long startMillis = System.currentTimeMillis();
        System.out.println("url is " + config.url());
        System.out.println("username: " + config.username());
        System.out.println("password: " + config.password());

        Connection con = null;
        try {
            // Class.forName(config.driver(), false, urlClassLoader);
            Class.forName(config.driver());
            System.out.println("=========================");
            con = DriverManager.getConnection(config.url(), config.username(), config.password());

        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
            System.out.println(e.getMessage());
        } catch (SQLException e){
            System.out.println("SQLException");
            System.out.println(e.getMessage());
        }
        long endMillis = System.currentTimeMillis();
        System.out.println("com.zoo.database.IMysql.getConnection耗时：" +  (endMillis - startMillis));
        System.out.println("con is: " + con);
        
        return con;
    }

    public void close(){
        try {
            if (this.con != null) {
                this.con.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.close();
    }

    public boolean execute(String sql){
        boolean ret = false;
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            if (stmt != null){
                ret = stmt.execute(sql);

                stmt.close();
            }
        }catch (Exception e){
            System.out.println("====================");
            System.out.println(sql);
            System.out.println("====================");
            System.out.println(e.getMessage());
        }

        return ret;
    }

    public List<Object[]> query(String sql) {
        ArrayList<Object[]> valueList = new ArrayList<>();

        try {
            Statement stmt = this.con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // 获得记录的详细信息，然后获得总列数
            ResultSetMetaData rsmd = rs.getMetaData();
            int colNum = rsmd.getColumnCount();
            // 用对象保存数据
            Object[] objArray = null;
            while (rs.next()) {
                objArray = new Object[colNum];
                for (int i = 0; i < colNum; i++) {
                    objArray[i] = rs.getObject(i + 1);
                }
                // 在valueList中加入这一行数据
                valueList.add(objArray);
            }
            // 释放数据库资源
            rs.close();
            stmt.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return valueList;
    }
}
