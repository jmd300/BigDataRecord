package com.zoo.grpc.Demo;

import com.zoo.grpc.api.RPCDateServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * @Author: JMD
 * @Date: 4/6/2023
 */
public class GrpcServer {
    private static final int port = 9999;

    public static void main(String[] args) throws Exception {
        // 设置service接口.
        Server server = ServerBuilder.
                forPort(port)
                .addService(new RPCDateServiceImpl())
                .build().start();
        System.out.println(String.format("GRpc服务端启动成功, 端口号: %d.", port));
        server.awaitTermination();
    }
}
