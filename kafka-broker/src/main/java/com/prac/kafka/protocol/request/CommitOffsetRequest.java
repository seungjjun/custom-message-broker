package com.prac.kafka.protocol.request;

public record CommitOffsetRequest(String consumerId, String topic, int partition, long offset) {
}
