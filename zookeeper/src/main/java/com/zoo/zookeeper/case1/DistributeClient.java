package com.zoo.zookeeper.case1;

import com.zoo.zookeeper.ZookeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    static final String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    static  final int sessionTimeout = 2000;

    static ZooKeeper zkClient;


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 获取zk连接
        DistributeClient client = new DistributeClient();
        client.getConnect();

        // 监听servers下面子节点的增加和删除
        // 不写这个就报错，KeeperErrorCode = NoNode for /servers
        client.getServerList();

        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/servers", true);
        ArrayList<String> servers = new ArrayList<>();

        for (String child: children){
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println("在线的服务器有：");
        System.out.println(servers);
    }

    void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher(){

            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
