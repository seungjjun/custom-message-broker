package com.prac.kafka.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {

    private final Map<OffsetKey, Long> offsets = new ConcurrentHashMap<>();

    public void commit(String consumerId, String topic, long offset) {
        OffsetKey key = OffsetKey.of(consumerId, topic);
        offsets.put(key, offset);
    }

    public long getCommitted(String consumerId, String topic) {
        OffsetKey key = OffsetKey.of(consumerId, topic);
        return offsets.getOrDefault(key, 0L);
    }
}
