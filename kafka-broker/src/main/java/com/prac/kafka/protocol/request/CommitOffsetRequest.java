package com.prac.kafka.protocol.request;

public record CommitOffsetRequest(String consumerId, String topic, long offset) {
}
