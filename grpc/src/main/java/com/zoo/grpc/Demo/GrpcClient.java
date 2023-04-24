package com.zoo.grpc.Demo;

import com.zoo.grpc.api.RPCDateRequest;
import com.zoo.grpc.api.RPCDateResponse;
import com.zoo.grpc.api.RPCDateServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @Author: JMD
 * @Date: 4/6/2023
 */
public class GrpcClient {
    private static final String host = "localhost";
    private static final int serverPort = 9999;

    public static void main(String[] args) throws Exception {
        // 1. 拿到一个通信的channel
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host, serverPort).usePlaintext().build();
        try {
            // 2.拿到代理对象
            RPCDateServiceGrpc.RPCDateServiceBlockingStub rpcDateService = RPCDateServiceGrpc.newBlockingStub(managedChannel);
            RPCDateRequest rpcDateRequest = RPCDateRequest
                    .newBuilder()
                    .setUserName("anthony")
                    .build();
            // 3. 请求
            RPCDateResponse rpcDateResponse = rpcDateService.getDate(rpcDateRequest);
            // 4. 输出结果
            System.out.println(rpcDateResponse.getServerDate());
        } finally {
            // 5.关闭channel, 释放资源.
            managedChannel.shutdown();
        }
    }
}
