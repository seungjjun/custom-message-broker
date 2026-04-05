package com.prac.kafka.protocol.response;

public record GetOffsetResponse(String consumerId, String topic, long committed) {
}
