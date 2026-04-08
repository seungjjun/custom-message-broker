package com.prac.kafka.protocol.response;

public record ProduceResponse(String topic, int partition, long offset) {
}
