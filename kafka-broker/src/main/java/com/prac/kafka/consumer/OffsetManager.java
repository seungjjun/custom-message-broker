package com.prac.kafka.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {

    private final Map<OffsetKey, Long> offsets = new ConcurrentHashMap<>();

    public void commit(String consumerId, String topic, int partition, long offset) {
        OffsetKey key = OffsetKey.of(consumerId, topic, partition);
        offsets.put(key, offset);
    }

    public long getCommitted(String consumerId, String topic, int partition) {
        OffsetKey key = OffsetKey.of(consumerId, topic, partition);
        return offsets.getOrDefault(key, 0L);
    }
}
