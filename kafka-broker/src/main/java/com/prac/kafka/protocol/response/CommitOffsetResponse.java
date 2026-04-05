package com.prac.kafka.protocol.response;

public record CommitOffsetResponse(String consumerId, String topic, long offset) {
}
