package com.zoo.zookeeper.case1;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

public class DistributeServer {
    static final String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    static  final int sessionTimeout = 2000;

    ZooKeeper zkClient;

    void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher(){

            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();
        //1. 获取zk连接
        server.getConnect();

        //2. 注册服务器到zk集群
        server.regist(args[0]);

        //等待
        Thread.sleep(Long.MAX_VALUE);
    }

    private void regist(String hostName) throws InterruptedException, KeeperException {
        System.out.println(hostName + "已经上线");

        // 如果不用变量接受返回结果就会报错 no node
        String create = zkClient.create("/servers/" + hostName, hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
}
