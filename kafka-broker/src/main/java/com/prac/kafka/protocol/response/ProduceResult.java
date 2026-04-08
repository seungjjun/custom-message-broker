package com.prac.kafka.protocol.response;

public record ProduceResult(int partition, long offset) {
}
