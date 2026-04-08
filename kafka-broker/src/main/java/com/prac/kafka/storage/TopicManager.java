package com.prac.kafka.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Map<String, PartitionedTopic> topics = new ConcurrentHashMap<>();

    public void createTopic(String topicName, int partitionCount) {
        PartitionedTopic existing = topics.putIfAbsent(topicName, new PartitionedTopic(topicName, partitionCount));
        if (existing != null) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
    }

    public PartitionedTopic getTopic(String topicName) {
        PartitionedTopic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist");
        }
        return topic;
    }
}
