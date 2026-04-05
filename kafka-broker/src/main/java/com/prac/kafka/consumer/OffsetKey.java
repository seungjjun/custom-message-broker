package com.prac.kafka.consumer;

public record OffsetKey(String consumerId, String topic) {

    public static OffsetKey of(String consumerId, String topic) {
        return new OffsetKey(consumerId, topic);
    }
}
