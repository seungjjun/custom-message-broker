package com.prac.kafka.protocol.response;

public record CreateTopicResponse(String topic, int partitions) {
}
