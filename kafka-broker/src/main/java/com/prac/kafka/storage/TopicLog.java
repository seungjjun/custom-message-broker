package com.prac.kafka.storage;

import com.prac.kafka.common.model.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TopicLog {

    private final List<Record> log = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public long append(String key, byte[] value) {
        lock.writeLock().lock();
        try {
            long offset = log.size();
            long currentTimestamp = System.currentTimeMillis();
            Record record = new Record(offset, currentTimestamp, key, value);

            log.add(record);
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Record> fetch(long fromOffset, long maxRecords) {
        lock.readLock().lock();
        try {
            int startIndex = (int) fromOffset;
            if (startIndex >= log.size() || startIndex < 0 || maxRecords <= 0) {
                return Collections.emptyList();
            }
            int endIndex = Math.min(startIndex + (int) maxRecords, log.size());
            return List.copyOf(log.subList(startIndex, endIndex));
        } finally {
            lock.readLock().unlock();
        }
    }
}
