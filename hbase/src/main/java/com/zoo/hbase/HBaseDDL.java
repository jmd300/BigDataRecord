package com.zoo.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class HBaseDDL {
    public static Connection connection = HBaseConnect.connection;

    public static void createNameSpace(String nameSpace) throws IOException {
        // 1. 获取 admin
        // 此处的异常先不要抛出 等待方法写完 再统一进行处理
        // admin 的连接是轻量级的 不是线程安全的 不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();

        // 2. 调用方法创建命名空间
        // 代码相对 shell 更加底层 所以 shell 能够实现的功能 代码一定能实现
        // 所以需要填写完整的命名空间描述
        // 2.1 创建命令空间描述建造者 => 设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);

        // 2.2 给命令空间添加需求
        builder.addConfiguration("user","hadoop");

        // 2.3 使用 builder 构造出对应的添加完参数的对象 完成创建
        // 创建命名空间出现的问题 都属于本方法自身的问题 不应该抛出
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("命令空间已经存在");
            e.printStackTrace();
        }

        // 3. 关闭 admin
        admin.close();
    }


    public static boolean isTableExists(String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName table = TableName.valueOf(nameSpace, tableName);
        boolean ret = false;
        try {
            ret = admin.tableExists(table);
        }catch (IOException e){
            e.printStackTrace();
        }

        admin.close();
        return ret;
    }

    public static void main(String[] args) throws IOException {
        // createNameSpace("hive2");
        boolean ret = isTableExists("big_data",  "student");
        System.out.println("ret is: " + ret);
        HBaseConnect.closeConnection();
    }
}
