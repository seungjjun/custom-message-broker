package com.prac.kafka.consumer;

public record OffsetKey(String consumerId, String topic, int partition) {

    public static OffsetKey of(String consumerId, String topic, int partition) {
        return new OffsetKey(consumerId, topic, partition);
    }
}
