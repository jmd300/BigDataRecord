package com.zoo.flink.java.pojo;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * @Author: JMD
 * @Date: 5/10/2023
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {}

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }


    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(user, event.user) && Objects.equals(url, event.url) && Objects.equals(timestamp, event.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, url, timestamp);
    }
}
