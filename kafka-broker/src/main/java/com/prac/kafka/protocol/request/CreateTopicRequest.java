package com.prac.kafka.protocol.request;

public record CreateTopicRequest(String topic, int partitions) {
}
