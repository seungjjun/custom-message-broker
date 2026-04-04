package com.prac.kafka.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Map<String, TopicLog> topics = new ConcurrentHashMap<>();

    public void createTopic(String topicName) {
        TopicLog existing = topics.putIfAbsent(topicName, new TopicLog());
        if (existing != null) {
            throw new IllegalArgumentException("Topic already exists: " + topicName);
        }
    }

    public TopicLog getTopicLog(String topicName) {
        TopicLog topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist");
        }
        return topic;
    }
}
