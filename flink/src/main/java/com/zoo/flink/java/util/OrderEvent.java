package com.zoo.flink.java.util;

import lombok.AllArgsConstructor;

/**
 * @Author: JMD
 * @Date: 6/20/2023
 */

@AllArgsConstructor
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long timestamp;

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
