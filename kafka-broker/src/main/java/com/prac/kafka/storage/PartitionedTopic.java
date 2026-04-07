package com.prac.kafka.storage;

import java.util.concurrent.atomic.AtomicInteger;

public class PartitionedTopic {

    private final String name;
    private final Partition[] partitions;
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public PartitionedTopic(String name, int partitionCount) {
        this.name = name;
        Partition[] partitions = new Partition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitions[i] = new Partition(i);
        }
        this.partitions = partitions;
    }

    public int selectPartition(String key) {
        if (key == null) {
            return roundRobinCounter.getAndIncrement() % partitions.length;
        }
        return (key.hashCode() & 0x7FFFFFFF) % partitions.length;
    }

    public Partition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public int partitionCount() {
        return partitions.length;
    }
}
