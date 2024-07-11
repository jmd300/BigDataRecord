package com.zoo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author: JMD
 * @Date: 8/13/2023
 */
public class ZkClient {
    static final String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    static  final int sessionTimeout = 2000;

    ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher(){

            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

                assert children != null;
                System.out.println("-----------process-------------");
                children.forEach(System.out::println);
                System.out.println("-----------process over--------");
            }
        });
    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        String s = zkClient.create("/date", "20240514".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("ret is " + s);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);

        System.out.println("-----------getChildren-------------");
        children.forEach(System.out::println);
        System.out.println("------------getChildren over-------");

        TimeUnit.MILLISECONDS.sleep(Long.MAX_VALUE);
    }
    
    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hadoop", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
