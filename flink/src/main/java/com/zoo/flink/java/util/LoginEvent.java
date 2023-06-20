package com.zoo.flink.java.util;

import lombok.AllArgsConstructor;

/**
 * @Author: JMD
 * @Date: 6/20/2023
 */
@AllArgsConstructor
public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long timestamp;

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
