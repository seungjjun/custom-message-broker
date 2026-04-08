package com.prac.kafka.client;

import com.prac.kafka.protocol.request.ProduceMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class BatchAccumulator {

    private long lastSendTime = System.currentTimeMillis();
    private final int batchSize;
    private final long lingerMs;
    private final List<ProduceMessage> buffer = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();

    public BatchAccumulator(int batchSize, long lingerMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
    }

    public Optional<List<ProduceMessage>> add(String key, String value) {
        lock.lock();
        try {
            ProduceMessage message = new ProduceMessage(key, value);
            buffer.add(message);

            if (buffer.size() >= batchSize) {
                List<ProduceMessage> batch = new ArrayList<>(buffer);
                buffer.clear();
                lastSendTime = System.currentTimeMillis();
                return Optional.of(batch);
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public Optional<List<ProduceMessage>> flush() {
        lock.lock();
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (!buffer.isEmpty() && currentTimeMillis - lastSendTime >= lingerMs) {
                List<ProduceMessage> batch = new ArrayList<>(buffer);
                buffer.clear();
                lastSendTime = System.currentTimeMillis();
                return Optional.of(batch);
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public Optional<List<ProduceMessage>> forceFlush() {
        lock.lock();
        try {
            if (!buffer.isEmpty()) {
                List<ProduceMessage> batch = new ArrayList<>(buffer);
                buffer.clear();
                lastSendTime = System.currentTimeMillis();
                return Optional.of(batch);
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public long getLingerMs() {
        return lingerMs;
    }
}
