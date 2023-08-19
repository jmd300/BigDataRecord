package com.zoo.flink.java.hive;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author: JMD
 * @Date: 7/17/2023
 */
public class UseHiveCatalogDemo {
    public static void main(String[] args) {

        // 创建 TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建 Hive Catalog
        String catalogName = "MyHiveCatalog";
        String defaultDatabase = "default";
        String hiveConfDir = "/opt/module/hive-3.1.2/conf/";  // Hive 配置文件目录
        Catalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);

        // 注册 Hive Catalog
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        tableEnv.useCatalog(catalogName);
    }
}
