package com.zoo.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public  class ZookeeperClient {
    static final String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    static  final int sessionTimeout = 2000;

    static ZooKeeper zkClient;

    public static synchronized ZooKeeper getClient(Watcher watcher) throws IOException {
        if(zkClient == null){
            Watcher watcher2 = watcher;
            if(watcher2 == null){
                watcher2 = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {

                    }
                };
            }
            zkClient = new ZooKeeper(connectionString, sessionTimeout, watcher2);
        }
        return zkClient;
    }
}
