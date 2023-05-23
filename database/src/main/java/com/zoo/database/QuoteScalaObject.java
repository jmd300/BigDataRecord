package com.zoo.database;

/**
 * @Author: JMD
 * @Date: 5/19/2023
 */
public class QuoteScalaObject {
    public static void main(String[] args) {
        String ret = DatabaseServer.getMysqlServer();
        System.out.println(ret);

        IMysql lionfulServer = MysqlServer.getLionfulServer();
        System.out.println(lionfulServer);
    }
}
