package com.prac.kafka.protocol.request;

public record FetchRequest(String topic, long offset, long maxRecords) {
}
