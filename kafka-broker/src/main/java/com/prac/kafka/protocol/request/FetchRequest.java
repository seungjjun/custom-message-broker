package com.prac.kafka.protocol.request;

public record FetchRequest(String topic, int partition, long offset, long maxRecords) {
}
