package com.zoo.zookeeper;

import org.apache.zookeeper.*;
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
    String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;

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
                System.out.println("------------------------");
                children.forEach(System.out::println);
                System.out.println("------------------------");
            }
        });
    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        String s = zkClient.create("/date_", "20230819".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("ret is " + s);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);

        System.out.println("------------------------");
        children.forEach(System.out::println);
        System.out.println("------------------------");

        TimeUnit.MILLISECONDS.sleep(Long.MAX_VALUE);
    }

}
