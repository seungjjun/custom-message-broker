package com.prac.kafka.protocol.request;

public record GetOffsetRequest(String consumerId, String topic, int partition) {
}
