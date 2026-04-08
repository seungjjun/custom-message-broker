package com.prac.kafka.protocol.response;

public record CommitOffsetResponse(String consumerId, String topic, int partition, long offset) {
}
